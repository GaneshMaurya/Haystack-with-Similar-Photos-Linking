import os, httpx, uuid, json, pika, logging
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response
from typing import Optional
import uvicorn
from logging.handlers import RotatingFileHandler

# Setup logging
log_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(log_dir, 'api.log')
os.makedirs(log_dir, exist_ok=True)

logger = logging.getLogger('api')
logger.setLevel(logging.DEBUG)
handler = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

app = FastAPI()
DIRECTORY_URL = os.environ.get("DIRECTORY_URL", "http://localhost:8080")
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

def make_store_url(store_id: str, port: int = 8101):
    """
    store_id may be 'localhost' OR 'localhost:8101'.
    This normalizes it to a full URL.
    """
    if ":" in store_id:
        return f"http://{store_id}"
    return f"http://{store_id}:8101"

# helper to publish event
def publish_event(payload):
    try:
        params = pika.URLParameters(RABBITMQ_URL)
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        ch.exchange_declare(exchange='photo.events', exchange_type='topic', durable=True)
        ch.basic_publish(
            exchange='photo.events',
            routing_key='photo.uploaded',
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        conn.close()
        logger.info(f"Event published: {payload['event']} for photo_id {payload['photo_id']}")
    except Exception as e:
        logger.error(f"Warning: Failed to publish event: {e}")

@app.post("/upload")
async def upload(file: UploadFile = File(...), alt_key: Optional[str] = "orig"):
    logger.info(f"Upload request received for file: {file.filename}")
    data = await file.read()
    size = len(data)
    logger.debug(f"File size: {size} bytes")
    
    async with httpx.AsyncClient() as client:
        # 1. allocate
        logger.debug(f"Allocating storage for {size} bytes")
        r = await client.post(f"{DIRECTORY_URL}/allocate_write", json={"size": size, "alt_key": alt_key})
        if r.status_code != 200:
            logger.error(f"Allocate failed: {r.text}")
            raise HTTPException(status_code=500, detail=f"allocate failed: {r.text}")
        alloc = r.json()
        logger.debug(f"Allocation successful: {alloc}")
        
        primary = alloc["primary_store"]
        cookie = alloc["cookie"]
        lv = alloc["logical_volume"]
        primary_host = f"http://{primary}" if "http" not in primary else primary
        
        # 2. append to primary
        photo_id = int(uuid.uuid4().int & (1<<31)-1)
        logger.info(f"Generated photo_id: {photo_id} for primary store: {primary}")
        headers = {"X-Photo-ID": str(photo_id), "X-Cookie": cookie}
        r2 = await client.post(f"{primary_host}/volume/{lv}/append", content=data, headers=headers, timeout=30.0)
        if r2.status_code != 200:
            logger.error(f"Store append failed: {r2.text}")
            raise HTTPException(status_code=500, detail=f"store append failed: {r2.text}")
        res = r2.json()
        offset = res["offset"]
        checksum = res["checksum"]
        logger.info(f"Photo appended to primary store at offset: {offset}, checksum: {checksum}")
        
        # 3. commit write
        logger.debug(f"Committing write to directory")
        commit_payload = {
            "photo_id": photo_id,
            "logical_volume": lv,
            "offset": offset,
            "size": size,
            "checksum": checksum,
            "cookie": cookie,
            "replicas": alloc["replicas"]
        }
        rc = await client.post(f"{DIRECTORY_URL}/commit_write", json=commit_payload)
        if rc.status_code != 200:
            logger.error(f"Commit write failed: {rc.text}")
            raise HTTPException(status_code=500, detail=f"commit failed: {rc.text}")
        logger.info(f"Upload successful: photo_id={photo_id}, size={size}, replicas={alloc['replicas']}")
    
    # 4. publish event (outside async context)
    publish_event({
        "event": "photo.uploaded",
        "photo_id": photo_id,
        "lv": lv,
        "source_store": primary,
        "offset": offset,
        "size": size,
        "checksum": checksum,
        "replicas": alloc["replicas"]
    })
    return {"photo_id": photo_id, "logical_volume": lv, "cookie": cookie}

@app.get("/photo/{photo_id}")
async def serve_photo(photo_id: int, alt: Optional[str] = "orig"):
    logger.info(f"Serve photo request: photo_id={photo_id}")
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{DIRECTORY_URL}/photo/{photo_id}")
        if r.status_code != 200:
            logger.warning(f"Photo not found: photo_id={photo_id}")
            raise HTTPException(status_code=404, detail="Photo not found")
        meta = r.json()
        logger.debug(f"Photo metadata retrieved: {meta}")

    lv = meta["logical_volume"]
    replicas = meta["replicas"]
    cookie = meta["cookie"]

    if meta["status"] != "active":
        logger.warning(f"Photo is deleted: photo_id={photo_id}")
        raise HTTPException(status_code=410, detail="Photo deleted")

    last_error = None

    for store in replicas:
        store_url = make_store_url(store)

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{store_url}/volume/{lv}/read",
                    params={"photo_id": photo_id},
                    timeout=10.0
                )

            if resp.status_code == 200:
                logger.info(f"Photo served successfully from {store}")
                return Response(
                    content=resp.content,
                    media_type="image/jpeg"
                )
            else:
                last_error = f"Store {store} responded {resp.status_code}"
                logger.warning(f"Store {store} responded with {resp.status_code}")

        except Exception as e:
            last_error = f"Store {store} error: {e}"
            logger.error(f"Store {store} error: {e}")
            continue

    logger.error(f"All replicas failed for photo_id={photo_id}")
    raise HTTPException(
        status_code=503,
        detail=f"All replicas failed: {last_error}"
    )

@app.delete("/photo/{photo_id}")
async def delete_photo(photo_id: int):
    """
    Delete photo across entire Haystack volume.
    Steps:
    1. Ask Directory for metadata
    2. Directory marks photo as deleted
    3. API sends delete command to all replica stores
    4. Return success
    """
    logger.info(f"Delete request for photo_id={photo_id}")
    async with httpx.AsyncClient() as client:
        # 1) Lookup metadata first
        r = await client.get(f"{DIRECTORY_URL}/photo/{photo_id}")
        if r.status_code != 200:
            logger.warning(f"Delete failed: Photo not found, photo_id={photo_id}")
            raise HTTPException(status_code=404, detail="Photo not found")
        meta = r.json()

        if meta["status"] == "deleted":
            logger.info(f"Photo already deleted: photo_id={photo_id}")
            return {"status": "already_deleted", "photo_id": photo_id}

        lv = meta["logical_volume"]
        replicas = meta["replicas"]

        # 2) Mark Directory as deleted
        logger.debug(f"Marking photo as deleted in directory: photo_id={photo_id}")
        rd = await client.post(
            f"{DIRECTORY_URL}/delete",
            json={"photo_id": photo_id}
        )
        if rd.status_code != 200:
            logger.error(f"Directory delete failed for photo_id={photo_id}")
            raise HTTPException(status_code=500, detail="Directory delete failed")

        # 3) Send delete to each replica store
        logger.debug(f"Deleting photo from {len(replicas)} replica stores")
        delete_failures = []

        for store in replicas:
            store_url = make_store_url(store)
            try:
                logger.debug(f"Deleting photo_id={photo_id} from store {store}")
                rs = await client.post(
                    f"{store_url}/volume/{lv}/delete",
                    json={"photo_id": photo_id},
                    timeout=10.0
                )
                if rs.status_code != 200:
                    delete_failures.append(store)
                    logger.warning(f"Delete from store {store} returned {rs.status_code}")
                else:
                    logger.debug(f"Successfully deleted photo_id={photo_id} from store {store}")
            except Exception as e:
                delete_failures.append(store)
                logger.error(f"Delete from store {store} failed: {e}")

        # 4) return response
        if delete_failures:
            logger.warning(f"Partial delete success for photo_id={photo_id}, failed stores: {delete_failures}")
            return {
                "status": "partial_success",
                "photo_id": photo_id,
                "failed_replicas": delete_failures,
                "reason": "Will be fixed by compactor eventually"
            }

        logger.info(f"Photo successfully deleted: photo_id={photo_id}")
        return {"status": "deleted", "photo_id": photo_id}

@app.get("/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)