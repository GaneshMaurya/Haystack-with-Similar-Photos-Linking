import os, json, time, requests
import pika

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
DIRECTORY_URL = os.environ.get("DIRECTORY_URL", "http://directory:8001")

params = pika.URLParameters(RABBITMQ_URL)
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.exchange_declare(exchange='photo.events', exchange_type='topic', durable=True)

result = channel.queue_declare(queue='replicator_queue', durable=True)
channel.queue_bind(exchange='photo.events', queue='replicator_queue', routing_key='photo.uploaded')

def callback(ch, method, properties, body):
    try:
        msg = json.loads(body)
        print("Replicator received:", msg)
        source = msg["source_store"]
        lv = msg["lv"]
        offset = msg["offset"]
        size = msg["size"]
        photo_id = msg["photo_id"]
        # fetch bytes from source store
        src_host = f"http://{source}:8101" if ":" not in source else f"http://{source}"
        # our store read supports read by photo_id (starter)
        r = requests.get(f"{src_host}/volume/{lv}/read", params={"photo_id": photo_id}, timeout=20)
        if r.status_code != 200:
            print("failed to fetch from source", r.status_code)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return
        blob = r.content
        # push to each replica
        for replica in msg.get("replicas", []):
            # skip if same as source
            if replica == source:
                continue
            host = f"http://{replica}:8101" if ":" not in replica else f"http://{replica}"
            # check exists first
            try:
                ex = requests.get(f"{host}/volume/{lv}/exists", params={"photo_id": photo_id}, timeout=5).json()
                if ex.get("exists"):
                    print(f"{photo_id} already on {replica}, skipping")
                    continue
            except Exception:
                pass
            headers = {"X-Photo-ID": str(photo_id), "X-Cookie": msg.get("cookie","")}
            r2 = requests.post(f"{host}/volume/{lv}/append", data=blob, headers=headers, timeout=20)
            if r2.status_code == 200:
                print("replicated to", replica)
                # optionally call directory /replication_update
                try:
                    requests.post(f"{DIRECTORY_URL}/replication_update", json={"photo_id": photo_id, "target": replica, "status":"complete"})
                except:
                    pass
            else:
                print("replicate failed to", replica, r2.status_code)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print("replicator exception", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='replicator_queue', on_message_callback=callback)
print("Replicator started, waiting for messages...")
channel.start_consuming()
