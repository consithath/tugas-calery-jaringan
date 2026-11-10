"""Demo skema message handling di RabbitMQ: fan-out, topic, wildcard.

Satu skrip, tanpa worker: pesan dipublikasikan ke exchange, lalu isi tiap
queue dibaca untuk melihat queue mana yang menerima pesan apa.

Jalankan:
  python exchanges/messaging_demo.py fanout
  python exchanges/messaging_demo.py topic
  python exchanges/messaging_demo.py wildcard
  python exchanges/messaging_demo.py all
"""
import os
import sys

from kombu import Connection, Exchange, Producer, Queue

URL = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")

# nama skema -> (tipe exchange, {queue: binding key}, [routing key pesan])
SCENARIOS = {
    "fanout": (
        "fanout",
        {"worker1": "", "worker2": "", "worker3": ""},
        ["info.apa-saja", "app.long"],  # routing key DIABAIKAN oleh fanout
    ),
    "topic": (
        "topic",
        {"q_long": "app.long", "q_short": "app.short", "q_error": "log.error"},
        ["app.long", "app.short", "log.error", "app.long.urgent", "log.info"],
    ),
    "wildcard": (
        "topic",
        {
            "q_app_bintang": "app.*",        # tepat satu kata setelah 'app.'
            "q_app_pagar": "app.#",          # nol atau lebih kata setelah 'app.'
            "q_semua_error": "#.error",      # apa pun yang berakhir '.error'
            "q_long_apa_saja": "*.long",     # satu kata + '.long'
        },
        ["app.long", "app.short", "app.long.urgent", "app", "log.error", "db.query.error", "web.long"],
    ),
}


def jalankan(nama: str) -> None:
    tipe, bindings, kunci_pesan = SCENARIOS[nama]
    print(f"\n{'=' * 64}\nSKEMA: {nama.upper()}  (exchange type = {tipe})\n{'=' * 64}")
    print("Binding:")
    for q, key in bindings.items():
        print(f"  queue {q:<16} <- binding key '{key}'")

    with Connection(URL) as conn:
        ch = conn.channel()
        ex = Exchange(f"demo_{nama}", type=tipe, durable=False)
        bound_ex = ex(ch)
        bound_ex.declare()

        queues = {}
        for q, key in bindings.items():
            bq = Queue(f"demo_{nama}_{q}", exchange=ex, routing_key=key, durable=False)(ch)
            bq.declare()
            bq.purge()
            queues[q] = bq

        try:
            prod = Producer(ch, exchange=ex, serializer="json")
            print("\nPesan dikirim:")
            for key in kunci_pesan:
                prod.publish({"routing_key": key}, routing_key=key)
                print(f"  publish routing_key='{key}'")

            print("\nHasil (isi tiap queue):")
            for q, bq in queues.items():
                diterima = []
                while (msg := bq.get(no_ack=True)) is not None:
                    diterima.append(msg.payload["routing_key"])
                print(f"  {q:<16} menerima {len(diterima)}: {diterima}")
        finally:
            for bq in queues.values():
                bq.delete()
            bound_ex.delete()


if __name__ == "__main__":
    pilihan = sys.argv[1] if len(sys.argv) > 1 else "all"
    if pilihan == "all":
        for s in SCENARIOS:
            jalankan(s)
    elif pilihan in SCENARIOS:
        jalankan(pilihan)
    else:
        sys.exit(f"Skema tidak dikenal: {pilihan}. Pilih: {', '.join(SCENARIOS)} atau all")
