"""Konfigurasi Celery untuk migrasi program UTS (load balancing socket) ke job queue.

Pemetaan UTS -> Celery:
  broker.py (socket)   -> RabbitMQ + routing queue
  worker.py (3 proses) -> 3 worker Celery (w1, w2, w3)
  Application ID 1     -> task long_task  (queue "long")
  Application ID 2     -> task short_task (queue "short")
  client.py            -> producer.py
"""
import os

from celery import Celery
from kombu import Exchange, Queue

BROKER_URL = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")

app = Celery(
    "uts_celery",
    broker=BROKER_URL,
    backend="rpc://",          # hasil dikembalikan ke producer lewat RabbitMQ
    include=["tasks"],
)

# Exchange bertipe direct: routing key == nama queue (Application ID -> queue)
uts_exchange = Exchange("uts", type="direct")

app.conf.task_queues = (
    Queue("long", uts_exchange, routing_key="long"),    # Application ID 1
    Queue("short", uts_exchange, routing_key="short"),  # Application ID 2
)

app.conf.task_routes = {
    "tasks.long_task": {"queue": "long", "routing_key": "long"},
    "tasks.short_task": {"queue": "short", "routing_key": "short"},
}

# Pembagian adil: worker hanya mengambil 1 pesan sekali waktu dan baru
# di-ack setelah selesai, sehingga worker yang lebih lapang mendapat task
# berikutnya (perilaku mirip mode "least" pada UTS).
app.conf.worker_prefetch_multiplier = 1
app.conf.task_acks_late = True

app.conf.task_serializer = "json"
app.conf.result_serializer = "json"
app.conf.accept_content = ["json"]
app.conf.timezone = "Asia/Jakarta"
