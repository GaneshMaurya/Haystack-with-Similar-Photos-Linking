"""
API Gateway (client-facing). Implements:
- POST /upload  -> allocate (Directory) -> append (primary store) -> commit (Directory) -> publish event
- GET  /photo/{photo_id} -> Directory lookup -> read from a replica store (fallback)
- DELETE /photo/{photo_id} -> Directory mark deleted -> send delete to replicas (best-effort)
"""

import os
import json
import httpx
from fastapi import FastAPI, File, UploadFile, HTTPException, Response
from typing import Optional
import pika
import base64
import logging

logging.basicConfig(level=logging.INFO)
app = FastAPI()

DIRECTORY_URL = os.environ.get("DIRECTORY_URL", "http://localhost:8001")
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
USE_RABBITMQ = os.environ.get("USE_RABBITMQ", "0") == "1"

_STORE_MAP_RAW = os.environ.get("STORE_PORTS", "store1=8101,store2=8102,store3=8103")
_STORE_MAP = {}
for kv in filter(None, _STORE_MAP_RAW.split(",")):
    if "=" in kv:
        k, v = kv.split("=", 1)
        _STORE_MAP[k.strip()] = v.strip()

# helper to publish events to RabbitMQ (durable)
def publish_event(payload: dict):
    if not USE_RABBITMQ:
        # running locally without RabbitMQ: just log the event to data/events.log
        try:
            os.makedirs("./data", exist_ok=True)
            with open("./data/events.log", "a") as f:
                f.write(json.dumps(payload) + "\n")
        except Exception:
            logging.exception("failed to write local event log")
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
        logging.exception("failed to publish event to rabbitmq")
        # For demo: swallow publish errors (but log in production)
        pass


def make_store_url(store_id: str):
    """
    Resolve a logical store id to a local URL. For local development the default mapping is:
      store1 -> http://localhost:8101
      store2 -> http://localhost:8102
      store3 -> http://localhost:8103

    You can override the mapping with STORE_PORTS env var: "store1=8101,store2=8102"
    If store_id already contains ':' it's treated as host:port and used as-is.
    """
    if ":" in store_id:
        return f"http://{store_id}"
    port = _STORE_MAP.get(store_id)
    if port:
        return f"http://localhost:{port}"
    # fallback to default port 8101
    return f"http://localhost:8101"


@app.post("/upload")
async def upload(file: UploadFile = File(...), alt_key: Optional[str] = "orig"):
    data = await file.read()
    size = len(data)

    # 1) allocate with Directory -> Directory *allocates photo_id* and returns logical volume, cookie, stores
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{DIRECTORY_URL}/allocate_write", json={"size": size, "alt_key": alt_key}, timeout=10.0)
    except Exception as e:
        logging.exception("allocate_write request exception")
        raise HTTPException(status_code=500, detail=f"allocate failed: {e}")

    if r.status_code != 200:
        logging.error("allocate_write failed: status=%s body=%s", r.status_code, r.text)
        raise HTTPException(status_code=500, detail=f"allocate failed: {r.status_code} {r.text}")
    alloc = r.json()

    photo_id = alloc["photo_id"]
    lv = alloc["logical_volume"]
    cookie = alloc["cookie"]
    primary = alloc["primary_store"]
    replicas = alloc["replicas"]

    # 2) append to primary store (include the Directory-assigned photo_id in header)
    primary_url = make_store_url(primary)
    headers = {"X-Photo-ID": str(photo_id), "X-Cookie": cookie, "X-Alt-Key": alt_key}
    try:
        async with httpx.AsyncClient() as client:
            r2 = await client.post(f"{primary_url}/volume/{lv}/append", content=data, headers=headers, timeout=30.0)
    except Exception as e:
        logging.exception("store append exception to %s", primary_url)
        # attempt to mark the alloc row as failed could be added here
        raise HTTPException(status_code=500, detail=f"store append failed: {e}")

    if r2.status_code != 200:
        logging.error("store append returned %s: %s", r2.status_code, r2.text)
        raise HTTPException(status_code=500, detail=f"store append failed: {r2.status_code} {r2.text}")
    store_res = r2.json()

    # 3) commit in Directory (mark active & create replication_state)
    try:
        async with httpx.AsyncClient() as client:
            rc = await client.post(f"{DIRECTORY_URL}/commit_write", json={"photo_id": photo_id}, timeout=5.0)
    except Exception as e:
        logging.exception("commit_write request exception")
        raise HTTPException(status_code=500, detail=f"commit failed: {e}")

    if rc.status_code != 200:
        logging.error("commit_write returned %s: %s", rc.status_code, rc.text)
        raise HTTPException(status_code=500, detail=f"commit failed: {rc.status_code} {rc.text}")

    # 4) publish upload event so replicator can copy to replicas
    publish_event({
        "event": "photo.uploaded",
        "photo_id": photo_id,
        "lv": lv,
        "source_store": primary,
        "replicas": replicas
    })

    return {"photo_id": photo_id, "logical_volume": lv}


@app.get("/photo/{photo_id}")
async def serve_photo(photo_id: int, alt: Optional[str] = "orig"):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{DIRECTORY_URL}/photo/{photo_id}")
        if r.status_code != 200:
            raise HTTPException(status_code=404, detail="Photo not found")
        meta = r.json()

    lv = meta["logical_volume"]
    replicas = meta["replicas"]
    cookie = meta.get("cookie")

    if meta.get("status") != "active":
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
                return Response(
                    content=resp.content,
                    media_type="image/jpeg"
                )
            else:
                last_error = f"Store {store} responded {resp.status_code}"

        except Exception as e:
            last_error = f"Store {store} error: {e}"
            continue

    raise HTTPException(
        status_code=503,
        detail=f"All replicas failed: {last_error}"
    )


@app.delete("/photo/{photo_id}")
async def delete_photo(photo_id: int):
    # 1) lookup logical metadata
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{DIRECTORY_URL}/photo/{photo_id}")
        if r.status_code != 200:
            raise HTTPException(status_code=404, detail="photo not found")
        meta = r.json()

    lv = meta["logical_volume"]
    replicas = meta["replicas"]

    # 2) mark deleted in Directory
    async with httpx.AsyncClient() as client:
        rd = await client.post(f"{DIRECTORY_URL}/delete", json={"photo_id": photo_id})
        if rd.status_code != 200:
            raise HTTPException(status_code=500, detail="directory delete failed")

    # 3) send delete to each replica (best-effort)
    failed = []
    for store in replicas:
        store_url = make_store_url(store)
        try:
            async with httpx.AsyncClient() as client:
                r2 = await client.post(f"{store_url}/volume/{lv}/delete", json={"photo_id": photo_id}, timeout=5.0)
            if r2.status_code != 200:
                failed.append(store)
        except Exception:
            failed.append(store)

    if failed:
        return {"status": "partial_deleted", "failed_replicas": failed}
    return {"status": "deleted", "photo_id": photo_id}
