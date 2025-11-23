"""
Haystack-style API Gateway (client-facing). Implements:
- POST /upload  -> allocate (Directory) -> append to ALL physical stores -> commit (Directory)
- POST /find_similar -> Query Similarity Service
- GET  /photo/{photo_id} -> Directory lookup -> Cache lookup -> Read from ANY replica
- DELETE /photo/{photo_id} -> Directory mark deleted -> delete from ALL replicas -> delete from cache
"""

import os
import json
import httpx
from fastapi import FastAPI, File, UploadFile, HTTPException, Response, Form # Added 'Form'
from typing import Optional
import pika
import logging
import uvicorn
import os

# Logging: write to console and a service-specific logfile under ./logs
LOG_DIR = os.environ.get("LOG_DIR", "./logs")
os.makedirs(LOG_DIR, exist_ok=True)
API_LOG_FILE = os.path.join(LOG_DIR, "api.log")

# Configure root logger only if not already configured
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
if not any(isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', None) == os.path.abspath(API_LOG_FILE) for h in root_logger.handlers):
    fh = logging.FileHandler(API_LOG_FILE)
    fh.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s'))
    root_logger.addHandler(fh)
app = FastAPI()

# Single entrypoint for all Directory operations (LB behind)
DIRECTORY_URL = os.environ.get("DIRECTORY_URL", "http://localhost:8001")

CACHE_URL = os.environ.get("CACHE_URL", "http://localhost:8201")
SIMILARITY_URL = os.environ.get("SIMILARITY_URL", "http://localhost:8301") # IMPORTANT: Ensure this is correctly set
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
USE_RABBITMQ = os.environ.get("USE_RABBITMQ", "0") == "1"

# STORE_ID -> PORT mapping
_STORE_MAP_RAW = os.environ.get("STORE_PORTS", "store1=8101,store2=8102,store3=8103")
_STORE_MAP = {}
for kv in filter(None, _STORE_MAP_RAW.split(",")):
    if "=" in kv:
        k, v = kv.split("=", 1)
        _STORE_MAP[k.strip()] = v.strip()


def make_store_url(store_id: str):
    """
    Converts store ID to the correct URL (using Docker network name in containers).
    
    If 'port' is defined (e.g., 'store1:8101' in Docker Compose), we use that entire
    string as the network address. Otherwise, we assume local host port mapping.
    """
    if ":" in store_id:
        return f"http://{store_id}"
    
    port_or_address = _STORE_MAP.get(store_id)
    
    if port_or_address:
        # If the environment variable provided a network address (e.g., 'store1:8101'), 
        # use the whole address prefixed by http://
        if ':' in port_or_address:
             return f"http://{port_or_address}"
        # If it only provided a port (e.g., '8101' in local testing), use localhost.
        return f"http://localhost:{port_or_address}"
        
    return "http://localhost:8101" # Default fallback


# ---------------- Publish Event -----------------

def publish_event(payload: dict):
    if not USE_RABBITMQ:
        os.makedirs("./data", exist_ok=True)
        with open("./data/events.log", "a") as f:
            f.write(json.dumps(payload) + "\n")
        return
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
    except Exception:
        logging.exception("failed to publish event")


# ---------------- UPLOAD (Haystack-style) -----------------

@app.post("/upload")
async def upload(file: UploadFile = File(...), alt_key: Optional[str] = "orig"):
    data = await file.read()
    size = len(data)

    # 1) allocate from Directory
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{DIRECTORY_URL}/allocate_write",
                              json={"size": size, "alt_key": alt_key},
                              timeout=10.0)
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=f"allocate_write failed: {r.text}")

    alloc = r.json()
    photo_id = alloc["photo_id"]
    logical_volume = alloc["logical_volume"]
    cookie = alloc["cookie"]
    replicas = alloc["replicas"]    # IMPORTANT: list of ALL physical stores

    headers = {"X-Photo-ID": str(photo_id),
               "X-Cookie": cookie,
               "X-Alt-Key": alt_key}

    # 2) Append synchronously to ALL stores (Haystack behavior)
    failed = []
    for store in replicas:
        store_url = make_store_url(store)
        try:
            async with httpx.AsyncClient() as sclient:
                r2 = await sclient.post(
                    f"{store_url}/volume/{logical_volume}/append",
                    content=data,
                    headers=headers,
                    timeout=30.0
                )
            if r2.status_code != 200:
                # LOGGING ADDED: Store returned a non-200 HTTP error
                logging.error(f"Store {store} failed. Status: {r2.status_code}. Response: {r2.text}") 
                failed.append(store)
        except Exception as e:
            # LOGGING ADDED: Network error (e.g., connection refused, timeout)
            logging.error(f"Network error or exception occurred with store {store}: {e}") 
            failed.append(store)

    if failed:
        # Log the overall failure reason
        logging.error(f"Upload aborted because append failed on stores: {failed}")
        # Remove bad replicas from Directory?
        # For now: abort upload entirely (Haystack usually marks them disabled)
        raise HTTPException(status_code=500,
                            detail=f"append failed on: {failed}")

    # 3) Commit write in Directory
    async with httpx.AsyncClient() as client:
        rc = await client.post(f"{DIRECTORY_URL}/commit_write",
                               json={"photo_id": photo_id},
                               timeout=5.0)

    if rc.status_code != 200:
        raise HTTPException(status_code=500,
                            detail=f"commit_write failed: {rc.text}")
    
    # 4) Upload to similarity Service
    try:
        async with httpx.AsyncClient() as client:
            # Since 'data' holds the full content, we can use it directly.
            form_data = {'photo_id': str(photo_id)}
            file_data = {'file': (file.filename, data, file.content_type)}
            
            r_sim = await client.post(
                f"{SIMILARITY_URL}/upload/", 
                data=form_data, 
                files=file_data, 
                timeout=30.0
            )
            
            if r_sim.status_code == 200:
                logging.info(f"Successfully submitted photo {photo_id} to similarity service.")
            else:
                logging.warning(f"Similarity service returned status {r_sim.status_code} for photo {photo_id}. Response: {r_sim.text}")
    except Exception as e:
        logging.error(f"Failed to submit photo {photo_id} to similarity service: {e}")

    # 5) Publish event (optional)
    publish_event({
        "event": "photo.uploaded",
        "photo_id": photo_id,
        "logical_volume": logical_volume,
        "replicas": replicas
    })

    return {"photo_id": photo_id, "logical_volume": logical_volume}


# ---------------- FIND SIMILAR -----------------

@app.post("/find_similar")
async def find_similar(file: UploadFile = File(...), k: int = Form(5)):
    """
    Forwards a query image to the similarity service and returns the top K similar photo IDs.
    """
    logging.info(f"Received similarity search request for file: {file.filename} with k={k}")

    # 1. Read the file content for transmission
    try:
        content = await file.read()
    except Exception as e:
        logging.error(f"Failed to read content from query file: {e}")
        raise HTTPException(status_code=500, detail="Failed to read query file content.")
    
    try:
        async with httpx.AsyncClient() as client:
            
            # 2. Define the form fields (k)
            # k must be passed as a string in the form data, as expected by the Similarity Service
            form_data = {'k': str(k)}

            # 3. Define the file field
            # The structure for files is: {'field_name': (filename, content, content_type)}
            files = {
                'file': (file.filename, content, file.content_type)
            }
            
            # 4. Send the POST request to the Similarity Service
            r_sim = await client.post(
                f"{SIMILARITY_URL}/find_similar/",
                data=form_data, 
                files=files,
                timeout=60.0 # Use a longer timeout for search
            )
            
            # 5. Process the response
            if r_sim.status_code == 200:
                logging.info("Successfully received similar photos from similarity service.")
                return r_sim.json()
            else:
                # Log the error and raise an appropriate HTTP exception
                logging.error(f"Similarity service returned status {r_sim.status_code}. Response: {r_sim.text}")
                raise HTTPException(
                    status_code=502,
                    detail=f"Similarity Service Error: {r_sim.status_code} - {r_sim.text[:100]}..."
                )

    except httpx.RequestError as e:
        logging.error(f"Network error calling similarity service: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"Cannot reach similarity service: {e}"
        )


# ---------------- READ -----------------

@app.get("/photo/{photo_id}")
async def serve_photo(photo_id: int):
    # 1) Lookup metadata from Directory
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{DIRECTORY_URL}/photo/{photo_id}")
    if r.status_code != 200:
        raise HTTPException(status_code=404, detail="photo not found")

    meta = r.json()
    if meta["status"] != "active":
        raise HTTPException(status_code=410, detail="photo deleted")

    logical_volume = meta["logical_volume"]
    replicas = meta["replicas"]

    # 2) Cache lookup
    async with httpx.AsyncClient() as client:
        try:
            cresp = await client.get(f"{CACHE_URL}/cache/photo/{photo_id}", timeout=5.0)
            if cresp.status_code == 200:
                return Response(content=cresp.content,
                                media_type="application/octet-stream")
        except:
            pass

    # 3) Cache miss → Try Store replicas
    for store in replicas:
        store_url = make_store_url(store)
        try:
            async with httpx.AsyncClient() as sclient:
                resp = await sclient.get(
                    f"{store_url}/volume/{logical_volume}/read",
                    params={"photo_id": photo_id},
                    timeout=10.0
                )
        except:
            continue

        if resp.status_code == 200:
            # store in cache
            async with httpx.AsyncClient() as cclient:
                try:
                    await cclient.post(
                        f"{CACHE_URL}/cache/photo/{photo_id}",
                        content=resp.content,
                        timeout=5.0
                    )
                except:
                    pass

            return Response(content=resp.content,
                            media_type="application/octet-stream")

    raise HTTPException(status_code=503, detail="all replicas failed")


# ---------------- DELETE -----------------

@app.delete("/photo/{photo_id}")
async def delete_photo(photo_id: int):

    # 1) Directory lookup
    async with httpx.AsyncClient() as dclient:
        r = await dclient.get(f"{DIRECTORY_URL}/photo/{photo_id}")
    if r.status_code != 200:
        raise HTTPException(status_code=404, detail="photo not found")

    meta = r.json()
    logical_volume = meta["logical_volume"]
    replicas = meta["replicas"]

    # 2) Mark deleted in Directory
    async with httpx.AsyncClient() as pclient:
        rd = await pclient.post(f"{DIRECTORY_URL}/delete",
                                json={"photo_id": photo_id})
    if rd.status_code != 200:
        raise HTTPException(status_code=500, detail="directory delete failed")

    # 3) Delete on ALL stores
    failed = []
    for store in replicas:
        store_url = make_store_url(store)
        try:
            async with httpx.AsyncClient() as sclient:
                r2 = await sclient.post(
                    f"{store_url}/volume/{logical_volume}/delete",
                    json={"photo_id": photo_id},
                    timeout=5.0
                )
            if r2.status_code != 200:
                failed.append(store)
        except:
            failed.append(store)

    # 4) Purge cache
    try:
        async with httpx.AsyncClient() as cclient:
            await cclient.delete(f"{CACHE_URL}/cache/photo/{photo_id}", timeout=3.0)
    except:
        pass

    return {"status": "deleted", "failed_replicas": failed}


if __name__ == "__main__":
    uvicorn.run("services.store.main:app", host="0.0.0.0")