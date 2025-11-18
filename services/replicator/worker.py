"""
Replicator: consumes photo.uploaded events and copies the needle from primary -> replicas.

Protocol:
- Message payload:
  {
    "event": "photo.uploaded",
    "photo_id": <int>,
    "lv": "lv-1",
    "source_store": "store1",
    "replicas": ["store2","store3"]
  }

Behavior:
- For each replica:
  - Check if replica already has photo (GET /volume/{lv}/exists?photo_id=)
  - If not, GET bytes from primary: GET /volume/{lv}/read?photo_id=
  - POST bytes to replica: POST /volume/{lv}/append (X-Photo-ID header)
  - On success, POST /replication_update to Directory (optional)
- Use at-least-once delivery semantics; operations are idempotent.
"""

import os
import json
import time
import requests
import pika

DIRECTORY_URL = os.environ.get("DIRECTORY_URL", "http://localhost:8001")
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")


def make_store_url(store_id: str):
    if ":" in store_id:
        return f"http://{store_id}"
    return f"http://{store_id}:8101"


def process_message(msg: dict):
    photo_id = msg["photo_id"]
    lv = msg["lv"]
    source = msg["source_store"]
    replicas = msg.get("replicas", [])

    src_url = make_store_url(source)

    # fetch from primary
    try:
        r = requests.get(f"{src_url}/volume/{lv}/read", params={"photo_id": photo_id}, timeout=20)
        if r.status_code != 200:
            print("replicator: failed to read from primary", r.status_code)
            return False
        blob = r.content
    except Exception as e:
        print("replicator: error fetching from primary", e)
        return False

    for replica in replicas:
        if replica == source:
            continue
        host = make_store_url(replica)
        try:
            # check exists
            ex = requests.get(f"{host}/volume/{lv}/exists", params={"photo_id": photo_id}, timeout=5).json()
            if ex.get("exists"):
                print(f"replicator: {photo_id} already on {replica}, skip")
                # optionally update replication_state in Directory
                try:
                    requests.post(f"{DIRECTORY_URL}/replication_update", json={"photo_id": photo_id, "target": replica, "status": "complete"})
                except:
                    pass
                continue
        except Exception:
            pass

        # append to replica
        try:
            headers = {"X-Photo-ID": str(photo_id)}
            r2 = requests.post(f"{host}/volume/{lv}/append", data=blob, headers=headers, timeout=20)
            if r2.status_code == 200:
                print(f"replicated {photo_id} -> {replica}")
                try:
                    requests.post(f"{DIRECTORY_URL}/replication_update", json={"photo_id": photo_id, "target": replica, "status": "complete"})
                except:
                    pass
            else:
                print("replicate failed to", replica, r2.status_code)
        except Exception as e:
            print("replicate exception", e)
    return True


def main():
    params = pika.URLParameters(RABBITMQ_URL)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.exchange_declare(exchange="photo.events", exchange_type="topic", durable=True)
    q = ch.queue_declare(queue="replicator_queue", durable=True)
    ch.queue_bind(exchange="photo.events", queue="replicator_queue", routing_key="photo.uploaded")

    def callback(ch, method, props, body):
        try:
            msg = json.loads(body)
            print("replicator got:", msg)
            ok = process_message(msg)
            if ok:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                # requeue for retry later
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        except Exception as e:
            print("replicator exception", e)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=q.method.queue, on_message_callback=callback)
    print("Replicator listening for messages...")
    ch.start_consuming()


if __name__ == "__main__":
    main()
