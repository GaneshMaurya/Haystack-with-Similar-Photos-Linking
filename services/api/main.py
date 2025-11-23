"""
Haystack-style API Gateway (client-facing). Implements:
- POST /upload  -> Discovery Lookup (Leader) -> allocate -> append to ALL stores -> commit
- POST /find_similar -> Query Similarity Service
- GET  /photo/{photo_id} -> Directory (Load-Balanced Read) -> Cache lookup -> Read from ANY replica
- DELETE /photo/{photo_id} -> Discovery Lookup (Leader) -> Directory delete -> delete from ALL replicas -> delete from cache
"""

import os
import json
import httpx
import random
import logging
import uvicorn
import pika
from fastapi import FastAPI, File, UploadFile, HTTPException, Response, Form
from typing import Optional, List

# ============================================
# LOGGING CONFIGURATION
# ============================================

LOG_DIR = os.environ.get("LOG_DIR", "./logs")
os.makedirs(LOG_DIR, exist_ok=True)
API_LOG_FILE = os.path.join(LOG_DIR, "api.log")

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Remove any existing handlers to avoid duplicates
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# Add file handler
file_handler = logging.FileHandler(API_LOG_FILE)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(name)s - %(message)s')
file_handler.setFormatter(file_formatter)
root_logger.addHandler(file_handler)

# Add console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(name)s - %(message)s')
console_handler.setFormatter(console_formatter)
root_logger.addHandler(console_handler)

# Get logger for this module
logger = logging.getLogger(__name__)

logger.info("=" * 60)
logger.info("API Gateway Starting")
logger.info(f"Log file: {API_LOG_FILE}")
logger.info("=" * 60)

# ============================================
# FASTAPI APP INITIALIZATION
# ============================================

app = FastAPI(title="API Gateway")

# ============================================
# ENVIRONMENT CONFIGURATION
# ============================================

DISCOVERY_SERVICE_URL = os.environ.get("DISCOVERY_SERVICE_URL", "http://discovery:8501")
DIRECTORY_URL = os.environ.get("DIRECTORY_URL", "http://localhost:8080")
CACHE_URL = os.environ.get("CACHE_URL", "http://cache:8201")
SIMILARITY_URL = os.environ.get("SIMILARITY_URL", "http://similarity:8301")
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
USE_RABBITMQ = os.environ.get("USE_RABBITMQ", "0") == "1"

logger.info(f"DISCOVERY_SERVICE_URL: {DISCOVERY_SERVICE_URL}")
logger.info(f"DIRECTORY_URL: {DIRECTORY_URL}")
logger.info(f"CACHE_URL: {CACHE_URL}")
logger.info(f"SIMILARITY_URL: {SIMILARITY_URL}")
logger.info(f"USE_RABBITMQ: {USE_RABBITMQ}")

# STORE_ID -> PORT/ADDRESS mapping
_STORE_MAP_RAW = os.environ.get("STORE_PORTS", "store1=8101,store2=8102")
_STORE_MAP = {}
for kv in filter(None, _STORE_MAP_RAW.split(",")):
    if "=" in kv:
        k, v = kv.split("=", 1)
        _STORE_MAP[k.strip()] = v.strip()

logger.info(f"Store map: {_STORE_MAP}")

# ============================================
# INTERNAL HELPERS
# ============================================

def make_store_url(store_id: str) -> str:
    """
    Constructs the correct URL for a Store service based on the environment map.
    Handles both Docker network addresses (storeX:port) and local ports.
    """
    logger.debug(f"Resolving store URL for store_id: {store_id}")
    
    port_or_address = _STORE_MAP.get(store_id)
    
    if port_or_address:
        if ':' in port_or_address:
            url = f"http://{port_or_address}"
            logger.debug(f"Store {store_id} -> {url} (full address)")
            return url
        else:
            url = f"http://localhost:{port_or_address}"
            logger.debug(f"Store {store_id} -> {url} (local port)")
            return url
    
    logger.warning(f"Store {store_id} not in map, using default fallback")
    return "http://localhost:8101"


async def _get_directory_leader_url() -> str:
    """Queries the Discovery Service to find the current Directory Primary."""
    logger.info("Querying Discovery Service for Directory leader...")
    
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{DISCOVERY_SERVICE_URL}/leader", timeout=5.0)
            
            if r.status_code == 200:
                data = r.json()
                leader_url = data["url"]
                logger.info(f"Directory leader found: {leader_url}")
                return leader_url
            elif r.status_code == 503:
                logger.error("Directory leader election failed: No healthy instances available")
                raise HTTPException(status_code=503, detail="Directory leader election failed: No healthy instances available.")
            else:
                logger.error(f"Discovery service error: {r.status_code} - {r.text}")
                raise HTTPException(status_code=500, detail=f"Discovery service error: {r.text}")
    except httpx.RequestError as e:
        logger.error(f"Cannot connect to Discovery Service: {e}")
        raise HTTPException(status_code=503, detail="Service Discovery is unavailable.")


async def _get_directory_read_url() -> str:
    """Queries the Discovery Service for a list of healthy replicas and returns one randomly."""
    logger.info("Querying Discovery Service for Directory read replica...")
    
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{DISCOVERY_SERVICE_URL}/replicas", timeout=5.0)
            
            if r.status_code == 200:
                urls = r.json().get("urls", [])
                if not urls:
                    logger.error("No healthy Directory instances for reading")
                    raise HTTPException(status_code=503, detail="No healthy Directory instances for reading.")
                
                selected_url = random.choice(urls)
                logger.info(f"Directory read replica selected: {selected_url}")
                return selected_url
            else:
                logger.error(f"Discovery service error during read lookup: {r.status_code} - {r.text}")
                raise HTTPException(status_code=500, detail=f"Discovery service error during read lookup: {r.text}")
    except httpx.RequestError as e:
        logger.error(f"Cannot connect to Discovery Service for reads: {e}")
        raise HTTPException(status_code=503, detail="Service Discovery is unavailable for reads.")


def publish_event(payload: dict):
    """Publish event to RabbitMQ or local file."""
    logger.info(f"Publishing event: {payload.get('event', 'unknown')}")
    
    if not USE_RABBITMQ:
        try:
            os.makedirs("./data", exist_ok=True)
            with open("./data/events.log", "a") as f:
                f.write(json.dumps(payload) + "\n")
            logger.info(f"Event logged to ./data/events.log")
        except Exception as e:
            logger.error(f"Failed to write event to file: {e}")
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
        logger.info(f"Event published to RabbitMQ successfully")
    except Exception as e:
        logger.error(f"Failed to publish event to RabbitMQ: {e}")


# ============================================
# UPLOAD ENDPOINT
# ============================================

@app.post("/upload")
async def upload(file: UploadFile = File(...), alt_key: Optional[str] = "orig"):
    """
    Upload a photo: allocate -> append to all stores -> commit -> similarity indexing
    """
    logger.info(f"[UPLOAD] Starting upload for file: {file.filename}")
    
    try:
        data = await file.read()
        size = len(data)
        logger.info(f"[UPLOAD] File size: {size} bytes")

        # 1) Locate Primary Directory via Discovery Service
        directory_leader_url = await _get_directory_leader_url()
        logger.info(f"[UPLOAD] Step 1: Directory leader selected: {directory_leader_url}")
        
        # 2) Allocate from Directory Primary
        logger.info(f"[UPLOAD] Step 2: Calling allocate_write on {directory_leader_url}")
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"{directory_leader_url}/allocate_write",
                json={"size": size, "alt_key": alt_key},
                timeout=10.0
            )
        
        if r.status_code != 200:
            logger.error(f"[UPLOAD] allocate_write failed: {r.status_code} - {r.text}")
            raise HTTPException(status_code=500, detail=f"allocate_write failed: {r.text}")

        alloc = r.json()
        photo_id = alloc["photo_id"]
        logical_volume = alloc["logical_volume"]
        cookie = alloc["cookie"]
        replicas = alloc["replicas"]
        
        logger.info(f"[UPLOAD] Step 2 Complete: photo_id={photo_id}, lv={logical_volume}, replicas={replicas}")

        headers = {
            "X-Photo-ID": str(photo_id),
            "X-Cookie": cookie,
            "X-Alt-Key": alt_key
        }

        # 3) Append synchronously to ALL stores
        logger.info(f"[UPLOAD] Step 3: Appending to {len(replicas)} store(s)")
        failed: List[str] = []
        
        for idx, store in enumerate(replicas):
            store_url = make_store_url(store)
            logger.info(f"[UPLOAD] Step 3.{idx+1}/{len(replicas)}: Appending to {store} at {store_url}")
            
            try:
                async with httpx.AsyncClient() as sclient:
                    r2 = await sclient.post(
                        f"{store_url}/volume/{logical_volume}/append",
                        content=data,
                        headers=headers,
                        timeout=30.0
                    )
                
                if r2.status_code != 200:
                    logger.error(f"[UPLOAD] Store {store} append failed: {r2.status_code} - {r2.text}")
                    failed.append(store)
                else:
                    logger.info(f"[UPLOAD] Store {store} append successful")
                    
            except Exception as e:
                logger.error(f"[UPLOAD] Exception with store {store}: {e}")
                failed.append(store)

        if failed:
            logger.error(f"[UPLOAD] Append failed on stores: {failed}")
            raise HTTPException(status_code=500, detail=f"Append failed on: {failed}")

        logger.info(f"[UPLOAD] Step 3 Complete: All stores appended successfully")

        # 4) Commit write in Directory Primary
        logger.info(f"[UPLOAD] Step 4: Committing write to Directory")
        async with httpx.AsyncClient() as client:
            rc = await client.post(
                f"{directory_leader_url}/commit_write",
                json={"photo_id": photo_id},
                timeout=5.0
            )

        if rc.status_code != 200:
            logger.error(f"[UPLOAD] commit_write failed: {rc.status_code} - {rc.text}")
            raise HTTPException(status_code=500, detail=f"commit_write failed: {rc.text}")

        logger.info(f"[UPLOAD] Step 4 Complete: Write committed successfully")

        # 5) Upload to Similarity Service
        logger.info(f"[UPLOAD] Step 5: Submitting to similarity service")
        try:
            async with httpx.AsyncClient() as client:
                form_data = {'photo_id': str(photo_id)}
                file_data = {'file': (file.filename, data, file.content_type)}
                
                r_sim = await client.post(
                    f"{SIMILARITY_URL}/upload/",
                    data=form_data,
                    files=file_data,
                    timeout=30.0
                )
                
                if r_sim.status_code == 200:
                    logger.info(f"[UPLOAD] Step 5 Complete: Photo {photo_id} submitted to similarity service")
                else:
                    logger.warning(f"[UPLOAD] Similarity service returned {r_sim.status_code}: {r_sim.text}")
        except Exception as e:
            logger.error(f"[UPLOAD] Failed to submit to similarity service: {e}")

        # 6) Publish event
        logger.info(f"[UPLOAD] Step 6: Publishing event")
        publish_event({
            "event": "photo.uploaded",
            "photo_id": photo_id,
            "logical_volume": logical_volume,
            "replicas": replicas
        })

        logger.info(f"[UPLOAD] SUCCESS: photo_id={photo_id}")
        return {"photo_id": photo_id, "logical_volume": logical_volume}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[UPLOAD] UNEXPECTED ERROR: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Upload failed due to unexpected error")


# ============================================
# READ ENDPOINT
# ============================================

@app.get("/photo/{photo_id}")
async def serve_photo(photo_id: int):
    """
    Read a photo: lookup directory -> cache lookup -> store read
    """
    logger.info(f"[READ] Starting read for photo_id={photo_id}")
    
    try:
        # 1) Lookup Directory Read URL
        logger.info(f"[READ] Step 1: Querying Directory for metadata")
        directory_read_url = await _get_directory_read_url()
        
        # 2) Lookup metadata from Directory
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{directory_read_url}/photo/{photo_id}")
        
        if r.status_code != 200:
            logger.error(f"[READ] Photo {photo_id} not found in Directory: {r.status_code}")
            raise HTTPException(status_code=404, detail="photo not found")

        meta = r.json()
        
        if meta["status"] != "active":
            logger.error(f"[READ] Photo {photo_id} is not active, status={meta['status']}")
            raise HTTPException(status_code=410, detail="photo deleted")

        logical_volume = meta["logical_volume"]
        replicas = meta["replicas"]
        
        logger.info(f"[READ] Step 1 Complete: lv={logical_volume}, replicas={replicas}")

        # 3) Cache lookup
        logger.info(f"[READ] Step 2: Checking cache")
        async with httpx.AsyncClient() as client:
            try:
                cresp = await client.get(f"{CACHE_URL}/cache/photo/{photo_id}", timeout=5.0)
                if cresp.status_code == 200:
                    logger.info(f"[READ] Step 2 Complete: Cache HIT for photo {photo_id}")
                    return Response(content=cresp.content, media_type="application/octet-stream")
            except Exception as e:
                logger.info(f"[READ] Cache miss or error: {e}")

        logger.info(f"[READ] Step 2 Complete: Cache MISS")

        # 4) Cache miss → Try Store replicas
        logger.info(f"[READ] Step 3: Reading from stores")
        for idx, store in enumerate(replicas):
            store_url = make_store_url(store)
            logger.info(f"[READ] Step 3.{idx+1}/{len(replicas)}: Trying store {store} at {store_url}")
            
            try:
                async with httpx.AsyncClient() as sclient:
                    resp = await sclient.get(
                        f"{store_url}/volume/{logical_volume}/read",
                        params={"photo_id": photo_id},
                        timeout=10.0
                    )
                
                if resp.status_code == 200:
                    logger.info(f"[READ] Step 3 Complete: Store {store} HIT")
                    
                    # Store in cache (best effort)
                    try:
                        async with httpx.AsyncClient() as cclient:
                            await cclient.post(
                                f"{CACHE_URL}/cache/photo/{photo_id}",
                                content=resp.content,
                                timeout=5.0
                            )
                            logger.debug(f"[READ] Cached photo {photo_id}")
                    except Exception as e:
                        logger.warning(f"[READ] Failed to cache photo {photo_id}: {e}")

                    return Response(content=resp.content, media_type="application/octet-stream")
            except Exception as e:
                logger.warning(f"[READ] Store {store} failed: {e}")
                continue

        logger.error(f"[READ] All replicas failed for photo {photo_id}")
        raise HTTPException(status_code=503, detail="All replicas failed to serve photo")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[READ] UNEXPECTED ERROR: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Read failed due to unexpected error")


# ============================================
# DELETE ENDPOINT
# ============================================

@app.delete("/photo/{photo_id}")
async def delete_photo(photo_id: int):
    """
    Delete a photo: directory soft-delete -> delete from all stores -> cache purge
    """
    logger.info(f"[DELETE] Starting delete for photo_id={photo_id}")
    
    try:
        # 1) Locate Primary Directory
        logger.info(f"[DELETE] Step 1: Querying Discovery for Directory leader")
        directory_leader_url = await _get_directory_leader_url()
        
        # 2) Directory lookup
        logger.info(f"[DELETE] Step 2: Fetching metadata from Directory")
        async with httpx.AsyncClient() as dclient:
            r = await dclient.get(f"{directory_leader_url}/photo/{photo_id}")
        
        if r.status_code != 200:
            logger.error(f"[DELETE] Photo {photo_id} not found")
            raise HTTPException(status_code=404, detail="photo not found")

        meta = r.json()
        logical_volume = meta["logical_volume"]
        replicas = meta["replicas"]
        
        logger.info(f"[DELETE] Step 2 Complete: lv={logical_volume}, replicas={replicas}")

        # 3) Mark deleted in Directory Primary
        logger.info(f"[DELETE] Step 3: Marking photo as deleted in Directory")
        async with httpx.AsyncClient() as pclient:
            rd = await pclient.post(f"{directory_leader_url}/delete", json={"photo_id": photo_id})
        
        if rd.status_code != 200:
            logger.error(f"[DELETE] Directory delete failed: {rd.status_code}")
            raise HTTPException(status_code=500, detail="directory delete failed")

        logger.info(f"[DELETE] Step 3 Complete: Directory marked as deleted")

        # 4) Delete from ALL stores (Best effort)
        logger.info(f"[DELETE] Step 4: Deleting from {len(replicas)} store(s)")
        failed: List[str] = []
        
        for idx, store in enumerate(replicas):
            store_url = make_store_url(store)
            logger.info(f"[DELETE] Step 4.{idx+1}/{len(replicas)}: Deleting from {store}")
            
            try:
                async with httpx.AsyncClient() as sclient:
                    r2 = await sclient.post(
                        f"{store_url}/volume/{logical_volume}/delete",
                        json={"photo_id": photo_id},
                        timeout=5.0
                    )
                
                if r2.status_code != 200:
                    logger.warning(f"[DELETE] Store {store} delete failed: {r2.status_code}")
                    failed.append(store)
                else:
                    logger.info(f"[DELETE] Store {store} delete successful")
            except Exception as e:
                logger.warning(f"[DELETE] Exception with store {store}: {e}")
                failed.append(store)

        logger.info(f"[DELETE] Step 4 Complete: Delete attempts complete (failed={failed})")

        # 5) Purge cache
        logger.info(f"[DELETE] Step 5: Purging from cache")
        try:
            async with httpx.AsyncClient() as cclient:
                await cclient.delete(f"{CACHE_URL}/cache/photo/{photo_id}", timeout=3.0)
            logger.info(f"[DELETE] Step 5 Complete: Cache purged")
        except Exception as e:
            logger.warning(f"[DELETE] Cache purge failed: {e}")

        logger.info(f"[DELETE] SUCCESS: photo_id={photo_id}, failed_replicas={failed}")
        return {"status": "deleted", "failed_replicas": failed}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[DELETE] UNEXPECTED ERROR: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Delete failed due to unexpected error")


# ============================================
# SIMILARITY SEARCH ENDPOINT
# ============================================

@app.post("/find_similar")
async def find_similar(file: UploadFile = File(...), k: int = Form(5)):
    """
    Find similar photos: forward to similarity service
    """
    logger.info(f"[FIND_SIMILAR] Starting search for {file.filename} with k={k}")
    
    try:
        # 1. Read file content
        logger.info(f"[FIND_SIMILAR] Step 1: Reading query file")
        try:
            content = await file.read()
            logger.info(f"[FIND_SIMILAR] Step 1 Complete: Read {len(content)} bytes")
        except Exception as e:
            logger.error(f"[FIND_SIMILAR] Failed to read file: {e}")
            raise HTTPException(status_code=500, detail="Failed to read query file content.")
        
        # 2. Send to similarity service
        logger.info(f"[FIND_SIMILAR] Step 2: Forwarding to similarity service")
        async with httpx.AsyncClient() as client:
            form_data = {'k': str(k)}
            files = {'file': (file.filename, content, file.content_type)}
            
            r_sim = await client.post(
                f"{SIMILARITY_URL}/find_similar/",
                data=form_data,
                files=files,
                timeout=60.0
            )
            
            if r_sim.status_code == 200:
                logger.info(f"[FIND_SIMILAR] SUCCESS: Received results from similarity service")
                return r_sim.json()
            else:
                logger.error(f"[FIND_SIMILAR] Similarity service error: {r_sim.status_code} - {r_sim.text}")
                raise HTTPException(
                    status_code=502,
                    detail=f"Similarity Service Error: {r_sim.status_code}"
                )

    except httpx.RequestError as e:
        logger.error(f"[FIND_SIMILAR] Network error: {e}")
        raise HTTPException(status_code=503, detail=f"Cannot reach similarity service")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[FIND_SIMILAR] UNEXPECTED ERROR: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Search failed due to unexpected error")


# ============================================
# HEALTH CHECK
# ============================================

@app.get("/health")
def health():
    """Health check endpoint"""
    logger.debug("Health check requested")
    return {"status": "ok", "service": "api-gateway"}


# ============================================
# STARTUP/SHUTDOWN
# ============================================

@app.on_event("startup")
async def startup_event():
    logger.info("API Gateway startup complete")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("API Gateway shutting down")


# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    logger.info("Starting API Gateway with Uvicorn...")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_config=None)