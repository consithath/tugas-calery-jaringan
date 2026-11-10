# Job Queue dengan Celery - Migrasi Simulasi Load Balancing UTS

Mata Kuliah : Komputasi Berbasis Jaringan - S2 Informatika, Universitas Negeri Surabaya
Anggota     : _(isi nama & NIM kelompok)_

Program UTS (broker, 3 worker, dan client dengan socket TCP) dipindahkan ke **Celery + RabbitMQ**.
Distribusi request ke worker kini ditangani broker RabbitMQ dan worker Celery, bukan kode socket manual.

## 1. Pemetaan UTS ke Celery

| UTS | Celery (repo ini) |
|---|---|
| `broker.py` (socket, port 5000, pemilih worker) | RabbitMQ + routing queue (`celery_app.py`) |
| `worker.py 1/2/3` (port 6001-6003) | 3 worker Celery: `w1`, `w2`, `w3` |
| Application ID 1 - Long (sleep 3 detik) | `tasks.long_task` -> queue `long` |
| Application ID 2 - Short (echo) | `tasks.short_task` -> queue `short` |
| `client.py` | `producer.py` |
| Balasan `RES <server_id> <app_id> <hasil>` | dict hasil task `{server_id, app_id, result}` |
| `ERR ...` untuk app_id tidak valid | validasi di `producer.kirim()` |
| Mode `rr` / `least` | distribusi bawaan RabbitMQ; `prefetch_multiplier=1` + `acks_late` membuat worker yang lapang mendapat task berikutnya (mirip `least`) |

## 2. Persiapan

```bash
uv venv myvenv --python 3.10        # atau: python -m venv myenv
source myvenv/bin/activate
uv pip install -r requirements.txt

docker compose up -d                # RabbitMQ (UI: http://localhost:15672, guest/guest)
```

Broker dapat diubah lewat variabel lingkungan `CELERY_BROKER_URL`
(default `amqp://guest:guest@localhost:5672//`).

## 3. Menjalankan Program UTS versi Celery

Buka terminal terpisah (virtualenv aktif di semuanya):

```bash
celery -A celery_app worker -n w1@%h -Q long,short --concurrency=1 --loglevel=info
celery -A celery_app worker -n w2@%h -Q long,short --concurrency=1 --loglevel=info
celery -A celery_app worker -n w3@%h -Q long,short --concurrency=1 --loglevel=info
celery -A celery_app flower          # opsional, dashboard di http://localhost:5555

python producer.py                          # 6 request, app selang-seling 1,2,1,2,...
python producer.py -n 20 --pola acak        # 20 request acak
python producer.py --app 1 --data hitung    # satu request saja
```

`--concurrency=1` dipakai agar satu worker = satu pekerja, seperti worker UTS, sehingga
pembagian task antar worker mudah diamati.

## 4. Skema Message Handling: Fan-Out, Topic, Wildcard

```bash
python exchanges/messaging_demo.py all      # atau: fanout | topic | wildcard
```

Skrip membuat exchange dan queue, mem-publish beberapa pesan, lalu menampilkan isi tiap queue.

### Ringkasan perbedaan

| | Fan-Out | Topic | Wildcard |
|---|---|---|---|
| Tipe exchange | `fanout` | `topic` | `topic` (binding memakai `*` dan `#`) |
| Routing key | **Diabaikan** | Dicocokkan dengan binding key | Dicocokkan dengan **pola** |
| Hasil | Semua queue terikat menerima salinan | Hanya queue dengan binding yang cocok | Queue yang polanya cocok (bisa banyak) |
| Kegunaan | Broadcast ke semua worker | Rute spesifik per kategori | Rute fleksibel per kelompok kategori |

Aturan wildcard: `*` cocok tepat **satu** kata, `#` cocok **nol atau lebih** kata (kata dipisah titik).

### Hasil percobaan

> Ekspektasi berdasarkan aturan AMQP. **Ganti dengan keluaran asli** dari `messaging_demo.py` di mesin kalian.

**Fan-Out** - pesan `info.apa-saja` dan `app.long`:
`worker1`, `worker2`, `worker3` masing-masing menerima **kedua pesan** (routing key tidak berpengaruh).

**Topic** - binding `app.long`, `app.short`, `log.error`:

| Routing key pesan | Diterima oleh |
|---|---|
| `app.long` | `q_long` |
| `app.short` | `q_short` |
| `log.error` | `q_error` |
| `app.long.urgent` | (tidak ada, tidak ada binding yang cocok persis) |
| `log.info` | (tidak ada) |

**Wildcard** - binding `app.*`, `app.#`, `#.error`, `*.long`:

| Queue (binding) | Pesan yang diterima |
|---|---|
| `q_app_bintang` (`app.*`) | `app.long`, `app.short` |
| `q_app_pagar` (`app.#`) | `app.long`, `app.short`, `app.long.urgent`, `app` |
| `q_semua_error` (`#.error`) | `log.error`, `db.query.error` |
| `q_long_apa_saja` (`*.long`) | `app.long`, `web.long` |

Pengamatan: `app.*` tidak menangkap `app.long.urgent` (dua kata) maupun `app` (nol kata),
sedangkan `app.#` menangkap keduanya. Itulah beda utama `*` dan `#`.

## 5. Hasil Pengujian Program UTS versi Celery

> Isi dengan keluaran `producer.py` dan tangkapan layar Flower.

```
(tempel keluaran producer.py di sini)
```

### Analisis

- Task **Long** (3 detik) tidak menghalangi task **Short** karena worker lain tetap mengambil
  pesan dari queue; dengan `prefetch_multiplier=1` pesan tidak menumpuk pada worker yang sibuk.
- Berbeda dengan UTS, kegagalan worker ditangani broker: pesan tanpa `ack` (karena `acks_late`)
  dikembalikan ke queue dan dikerjakan worker lain.
- Pada UTS, logika `rr`/`least` ditulis manual dengan `lock`; di Celery logika itu diganti
  mekanisme konsumsi RabbitMQ sehingga tidak ada race condition pada kode aplikasi.

## 6. Struktur Repositori

```
celery_app.py      konfigurasi Celery, broker, queue, routing
tasks.py           long_task (App 1) dan short_task (App 2)
producer.py        input generator
exchanges/messaging_demo.py   demo fan-out, topic, wildcard
docker-compose.yml RabbitMQ
requirements.txt
```
