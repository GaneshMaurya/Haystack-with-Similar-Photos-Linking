import os
import json
import asyncio
import threading
import logging
import pika
import httpx
import uvicorn
import numpy as np
from fastapi import FastAPI, HTTPException
# Only import retrieval system if we are MASTER (to save RAM on workers)
# Only import feature extractor if we are WORKER (to save RAM on master)

# --- Configuration ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("similarity-service")

MODE = os.environ.get("SERVICE_MODE", "MASTER") # "MASTER" or "WORKER"
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
API_URL = os.environ.get("API_URL", "http://api:8080")
DATA_DIR = "/data/similarity_images"

# --- Shared RabbitMQ Helpers ---
def get_channel():
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    return connection, connection.channel()

# ==============================================================================
#                                   WORKER MODE
#          (Listens to photo.uploaded -> Computes Vector -> Sends to Master)
# ==============================================================================
if MODE == "WORKER":
    from feature_extractor import ImageFeatureExtractor
    
    # Initialize AI Model (Heavy CPU usage)
    logger.info("Initializing AI Model for Worker...")
    extractor = ImageFeatureExtractor()
    os.makedirs(DATA_DIR, exist_ok=True)

    def download_image(photo_id):
        local_path = os.path.join(DATA_DIR, f"{photo_id}.jpg")
        if os.path.exists(local_path): return local_path
        try:
            with httpx.Client() as client:
                resp = client.get(f"{API_URL}/photo/{photo_id}", timeout=30.0)
                if resp.status_code == 200:
                    with open(local_path, "wb") as f: f.write(resp.content)
                    return local_path
        except Exception as e:
            logger.error(f"Download failed: {e}")
        return None

    def worker_callback(ch, method, properties, body):
        try:
            event = json.loads(body)
            if event.get("event") == "photo.uploaded":
                photo_id = event.get("photo_id")
                logger.info(f"[Worker] Processing Photo {photo_id}...")
                
                # 1. Download
                path = download_image(photo_id)
                if path:
                    # 2. Compute Vector (Heavy Operation)
                    vector = extractor.extract_features(path)
                    
                    # 3. Send Vector to Master
                    payload = {
                        "event": "vector.ready",
                        "photo_id": photo_id,
                        "path": path,
                        "vector": vector.tolist() # Convert numpy to list for JSON
                    }
                    
                    # Publish to 'vector_queue'
                    ch.basic_publish(
                        exchange='',
                        routing_key='vector_queue',
                        body=json.dumps(payload),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    logger.info(f"[Worker] Vector sent for {photo_id}")

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"[Worker] Error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_worker():
        conn, ch = get_channel()
        ch.exchange_declare(exchange='photo.events', exchange_type='topic', durable=True)
        q = ch.queue_declare(queue='similarity_worker_queue', durable=True).method.queue
        ch.queue_bind(exchange='photo.events', queue=q, routing_key='photo.uploaded')
        # Also declare the destination queue so it exists
        ch.queue_declare(queue='vector_queue', durable=True)
        
        logger.info("[Worker] Ready to process photos...")
        ch.basic_consume(queue=q, on_message_callback=worker_callback)
        ch.start_consuming()

    if __name__ == "__main__":
        start_worker()


# ==============================================================================
#                                   MASTER MODE
#          (Listens to vector.ready -> Writes to FAISS -> Serves Search API)
# ==============================================================================
elif MODE == "MASTER":
    from retrieval_system import ImageRetrievalSystem
    
    INDEX_PATH = "/data/faiss.index"
    META_PATH = "/data/faiss_meta.json"
    
    # Initialize Database (Heavy RAM usage)
    logger.info("Initializing FAISS Master...")
    similarity_system = ImageRetrievalSystem(index_path=INDEX_PATH, metadata_path=META_PATH, use_gpu=False)
    
    app = FastAPI()

    # --- Batch Processor for Vectors ---
    def master_callback(ch, method, properties, body):
        try:
            event = json.loads(body)
            if event.get("event") == "vector.ready":
                photo_id = event["photo_id"]
                vector = np.array(event["vector"], dtype=np.float32)
                path = event["path"]
                
                # Write to FAISS (This is fast now because vector is already computed)
                # We need to manually add vector because add_single_image expects a file path to re-compute
                # Let's modify usage of retrieval system slightly or use internal index directly
                
                # Direct Index Injection
                similarity_system.index.add(np.array([vector]))
                new_id = similarity_system.index.ntotal - 1
                similarity_system.metadata[str(new_id)] = {
                    'photo_id': str(photo_id),
                    'path': path,
                    'timestamp': "now"
                }
                similarity_system.save(INDEX_PATH, META_PATH)
                logger.info(f"[Master] Indexed Photo {photo_id}")

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"[Master] Error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_master_consumer():
        conn, ch = get_channel()
        ch.queue_declare(queue='vector_queue', durable=True)
        logger.info("[Master] Listening for vectors...")
        ch.basic_consume(queue='vector_queue', on_message_callback=master_callback)
        ch.start_consuming()

    # Start consumer thread
    threading.Thread(target=start_master_consumer, daemon=True).start()

    @app.get("/similar/{photo_id}")
    def get_similar_photos(photo_id: str, k: int = 5):
        # Master keeps the 'find_path_by_id' logic, but needs to re-implement search 
        # because 'similarity_system.search' expects a file path to re-compute embedding.
        # Ideally, we should fetch embedding from DB, but FAISS IndexFlatL2 doesn't store IDs easily.
        # For this demo, we will use the logic: Get Path -> Re-compute? 
        # NO, Master shouldn't compute.
        # Strategy: Master will do a cheap re-compute or we store vectors in metadata (heavy).
        # Fallback: Master DOES compute only for query (low load), Workers do compute for indexing (high load).
        
        path = similarity_system.find_path_by_id(photo_id)
        if not path: raise HTTPException(404, "Not found")
        
        # Note: Master still needs feature_extractor for SEARCH queries.
        # To fix this properly, we lazily load extractor only on Master when searching.
        if not hasattr(app, "extractor"):
             from feature_extractor import ImageFeatureExtractor
             app.extractor = ImageFeatureExtractor()
        
        query_feat = app.extractor.extract_features(path)
        distances, indices = similarity_system.index.search(query_feat.reshape(1, -1), k)
        
        results = []
        for dist, idx in zip(distances[0], indices[0]):
            s_idx = str(int(idx))
            if s_idx in similarity_system.metadata:
                item = similarity_system.metadata[s_idx]
                if str(item['photo_id']) != str(photo_id):
                    results.append({"photo_id": item['photo_id'], "distance": float(dist)})
        
        return {"photo_id": photo_id, "similar": results}

    if __name__ == "__main__":
        uvicorn.run(app, host="0.0.0.0", port=8000)