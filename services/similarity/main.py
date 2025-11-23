import os
import json
import asyncio
import threading
import logging
import pika
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from retrieval_system import ImageRetrievalSystem

# --- Configuration ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("similarity-service")

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
API_URL = os.environ.get("API_URL", "http://api:8080")
DATA_DIR = "/data/similarity_images"
INDEX_PATH = "/data/faiss.index"
META_PATH = "/data/faiss_meta.json"

# Ensure data dir exists
os.makedirs(DATA_DIR, exist_ok=True)

# --- Initialize System ---
# We initialize the system you wrote. 
# We use n_regions=1 for simplicity if dataset is small initially, 
# or keep your default 100 if you expect many photos.
similarity_system = ImageRetrievalSystem(
    index_path=INDEX_PATH,
    metadata_path=META_PATH,
    use_gpu=False,
    n_regions=5  # Lower regions for small test data
)

# If index doesn't exist, we might need to "train" it with dummy data 
# or handle the first batch carefully. Your code handles "not trained" checks.

app = FastAPI()

# --- RabbitMQ Consumer (Background Thread) ---
def download_image(photo_id):
    """Downloads image from the Main API/Store to local disk for processing"""
    local_path = os.path.join(DATA_DIR, f"{photo_id}.jpg")
    
    # If we already have it, skip download
    if os.path.exists(local_path):
        return local_path

    try:
        # We call the Main API to get the photo bytes
        with httpx.Client() as client:
            resp = client.get(f"{API_URL}/photo/{photo_id}", timeout=30.0)
            if resp.status_code == 200:
                with open(local_path, "wb") as f:
                    f.write(resp.content)
                return local_path
            else:
                logger.error(f"Failed to download photo {photo_id}: {resp.status_code}")
                return None
    except Exception as e:
        logger.error(f"Exception downloading photo {photo_id}: {e}")
        return None

def callback(ch, method, properties, body):
    try:
        event = json.loads(body)
        logger.info(f"Received event: {event}")

        if event.get("event") == "photo.uploaded":
            photo_id = event.get("photo_id")
            
            # 1. Download image to local storage
            image_path = download_image(photo_id)
            
            if image_path:
                # 2. Auto-Train if needed (FAISS IVF needs training on first few images)
                if not similarity_system.is_trained:
                    logger.info("Index not trained. Training on single image (not optimal but functional for demo)...")
                    # Hack: For IVF index, we need a batch to train. 
                    # If we are adding one by one, we might need IndexFlatL2 (Brute force) instead of IVF 
                    # OR we force training on this one image.
                    # Ideally, your index_images logic handles batch, but here we do single.
                    # Let's re-use your batch logic for this single file to force training.
                    similarity_system.index_images(DATA_DIR)
                else:
                    # 3. Add to Index using your code
                    similarity_system.add_single_image(image_path, str(photo_id))
                
                logger.info(f"Successfully indexed photo {photo_id}")

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def start_consumer():
    try:
        params = pika.URLParameters(RABBITMQ_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        
        channel.exchange_declare(exchange='photo.events', exchange_type='topic', durable=True)
        result = channel.queue_declare(queue='similarity_queue', durable=True)
        queue_name = result.method.queue
        
        channel.queue_bind(exchange='photo.events', queue=queue_name, routing_key='photo.uploaded')
        
        logger.info("Similarity Consumer started...")
        channel.basic_consume(queue=queue_name, on_message_callback=callback)
        channel.start_consuming()
    except Exception as e:
        logger.error(f"Consumer connection failed: {e}")

# Start consumer in background thread
consumer_thread = threading.Thread(target=start_consumer, daemon=True)
consumer_thread.start()

# --- API Endpoints ---
@app.get("/similar/{photo_id}")
def get_similar_photos(photo_id: str, k: int = 5):
    print(f"DEBUG: Request received for photo {photo_id}") # <--- Debug 1
    
    query_path = similarity_system.find_path_by_id(photo_id)
    
    if not query_path:
        print(f"DEBUG: Not in index, trying download...") # <--- Debug 2
        query_path = download_image(photo_id)
        if not query_path:
             raise HTTPException(status_code=404, detail="Photo not found")

    print(f"DEBUG: Image at {query_path}. Starting Search...") # <--- Debug 3
    try:
        # This is likely where it crashes
        results = similarity_system.search(query_path, k=k) 
        print(f"DEBUG: Search success!") # <--- If you don't see this, it crashed above
        
        results = [r for r in results if str(r['photo_id']) != str(photo_id)]
        return {"photo_id": photo_id, "similar": results}
    except Exception as e:
         print(f"DEBUG: Error: {e}")
         raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")
@app.get("/health")
def health():
    return {"status": "ok", "trained": similarity_system.is_trained, "total_indexed": similarity_system.index.ntotal if similarity_system.is_trained else 0}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)