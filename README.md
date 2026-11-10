# Job Queue dengan Celery - Migrasi Simulasi Load Balancing UTS

Mata Kuliah : Komputasi Berbasis Jaringan - S2 Informatika, Universitas Negeri Surabaya
Anggota     : *Theresia Maria Consitha Hurint (26051905035) \& Trio Achma faradilah hadi (26051905005)*

Program UTS (broker, 3 worker, dan client dengan socket TCP) dipindahkan ke **Celery + RabbitMQ**.
Distribusi request ke worker kini ditangani broker RabbitMQ dan worker Celery, bukan kode socket manual.

## 1\. Pemetaan UTS ke Celery

|UTS|Celery (repo ini)|
|-|-|
|`broker.py` (socket, port 5000, pemilih worker)|RabbitMQ + routing queue (`celery\_app.py`)|
|`worker.py 1/2/3` (port 6001-6003)|3 worker Celery: `w1`, `w2`, `w3`|
|Application ID 1 - Long (sleep 3 detik)|`tasks.long\_task` -> queue `long`|
|Application ID 2 - Short (echo)|`tasks.short\_task` -> queue `short`|
|`client.py`|`producer.py`|
|Balasan `RES <server\_id> <app\_id> <hasil>`|dict hasil task `{server\_id, app\_id, result}`|
|`ERR ...` untuk app\_id tidak valid|validasi di `producer.kirim()`|
|Mode `rr` / `least`|distribusi bawaan RabbitMQ; `prefetch\_multiplier=1` + `acks\_late` membuat worker yang lapang mendapat task berikutnya (mirip `least`)|

## 2\. Persiapan

```bash
uv venv myvenv --python 3.10        # atau: python -m venv myenv
source myvenv/bin/activate
uv pip install -r requirements.txt

docker compose up -d                # RabbitMQ (UI: http://localhost:15672, guest/guest)
```

Broker dapat diubah lewat variabel lingkungan `CELERY\_BROKER\_URL`
(default `amqp://guest:guest@localhost:5672//`).

## 3\. Menjalankan Program UTS versi Celery

Buka terminal terpisah (virtualenv aktif di semuanya):

```bash
celery -A celery\_app worker -n w1@%h -Q long,short --concurrency=1 --loglevel=info
celery -A celery\_app worker -n w2@%h -Q long,short --concurrency=1 --loglevel=info
celery -A celery\_app worker -n w3@%h -Q long,short --concurrency=1 --loglevel=info
celery -A celery\_app flower          # opsional, dashboard di http://localhost:5555

python producer.py                          # 6 request, app selang-seling 1,2,1,2,...
python producer.py -n 20 --pola acak        # 20 request acak
python producer.py --app 1 --data hitung    # satu request saja
```

`--concurrency=1` dipakai agar satu worker = satu pekerja, seperti worker UTS, sehingga
pembagian task antar worker mudah diamati.

## 4\. Skema Message Handling: Fan-Out, Topic, Wildcard

```bash
python exchanges/messaging\_demo.py all      # atau: fanout | topic | wildcard
```

Skrip membuat exchange dan queue, mem-publish beberapa pesan, lalu menampilkan isi tiap queue.

### Ringkasan perbedaan

||Fan-Out|Topic|Wildcard|
|-|-|-|-|
|Tipe exchange|`fanout`|`topic`|`topic` (binding memakai `\*` dan `#`)|
|Routing key|**Diabaikan**|Dicocokkan dengan binding key|Dicocokkan dengan **pola**|
|Hasil|Semua queue terikat menerima salinan|Hanya queue dengan binding yang cocok|Queue yang polanya cocok (bisa banyak)|
|Kegunaan|Broadcast ke semua worker|Rute spesifik per kategori|Rute fleksibel per kelompok kategori|

Aturan wildcard: `\*` cocok tepat **satu** kata, `#` cocok **nol atau lebih** kata (kata dipisah titik).

### Hasil percobaan

>================================================================

SKEMA: FANOUT  (exchange type = fanout)

================================================================

Binding:

&#x20; queue worker1          <- binding key ''

&#x20; queue worker2          <- binding key ''

&#x20; queue worker3          <- binding key ''



Pesan dikirim:

&#x20; publish routing\_key='info.apa-saja'

&#x20; publish routing\_key='app.long'



Hasil (isi tiap queue):

&#x20; worker1          menerima 2: \['info.apa-saja', 'app.long']

&#x20; worker2          menerima 2: \['info.apa-saja', 'app.long']

&#x20; worker3          menerima 2: \['info.apa-saja', 'app.long']



================================================================

SKEMA: TOPIC  (exchange type = topic)

================================================================

Binding:

&#x20; queue q\_long           <- binding key 'app.long'

&#x20; queue q\_short          <- binding key 'app.short'

&#x20; queue q\_error          <- binding key 'log.error'



Pesan dikirim:

&#x20; publish routing\_key='app.long'

&#x20; publish routing\_key='app.short'

&#x20; publish routing\_key='log.error'

&#x20; publish routing\_key='app.long.urgent'

&#x20; publish routing\_key='log.info'



Hasil (isi tiap queue):

&#x20; q\_long           menerima 1: \['app.long']

&#x20; q\_short          menerima 1: \['app.short']

&#x20; q\_error          menerima 1: \['log.error']



================================================================

SKEMA: WILDCARD  (exchange type = topic)

================================================================

Binding:

&#x20; queue q\_app\_bintang    <- binding key 'app.\*'

&#x20; queue q\_app\_pagar      <- binding key 'app.#'

&#x20; queue q\_semua\_error    <- binding key '#.error'

&#x20; queue q\_long\_apa\_saja  <- binding key '\*.long'



Pesan dikirim:

&#x20; publish routing\_key='app.long'

&#x20; publish routing\_key='app.short'

&#x20; publish routing\_key='app.long.urgent'

&#x20; publish routing\_key='app'

&#x20; publish routing\_key='log.error'

&#x20; publish routing\_key='db.query.error'

&#x20; publish routing\_key='web.long'



Hasil (isi tiap queue):

&#x20; q\_app\_bintang    menerima 2: \['app.long', 'app.short']

&#x20; q\_app\_pagar      menerima 4: \['app.long', 'app.short', 'app.long.urgent', 'app']

&#x20; q\_semua\_error    menerima 2: \['log.error', 'db.query.error']

&#x20; q\_long\_apa\_saja  menerima 2: \['app.long', 'web.long'].

**Fan-Out** - pesan `info.apa-saja` dan `app.long`:
`worker1`, `worker2`, `worker3` masing-masing menerima **kedua pesan** (routing key tidak berpengaruh).

**Topic** - binding `app.long`, `app.short`, `log.error`:

|Routing key pesan|Diterima oleh|
|-|-|
|`app.long`|`q\_long`|
|`app.short`|`q\_short`|
|`log.error`|`q\_error`|
|`app.long.urgent`|(tidak ada, tidak ada binding yang cocok persis)|
|`log.info`|(tidak ada)|

**Wildcard** - binding `app.\*`, `app.#`, `#.error`, `\*.long`:

|Queue (binding)|Pesan yang diterima|
|-|-|
|`q\_app\_bintang` (`app.\*`)|`app.long`, `app.short`|
|`q\_app\_pagar` (`app.#`)|`app.long`, `app.short`, `app.long.urgent`, `app`|
|`q\_semua\_error` (`#.error`)|`log.error`, `db.query.error`|
|`q\_long\_apa\_saja` (`\*.long`)|`app.long`, `web.long`|

Pengamatan: `app.\*` tidak menangkap `app.long.urgent` (dua kata) maupun `app` (nol kata),
sedangkan `app.#` menangkap keduanya. Itulah beda utama `\*` dan `#`.

## 5\. Hasil Pengujian Program UTS versi Celery

> !\[Queue RabbitMQ](docs/rabbitmq-queues.png)

================================================================

SKEMA: WILDCARD  (exchange type = topic)

================================================================

Binding:

&#x20; queue q\_app\_bintang    <- binding key 'app.\*'

&#x20; queue q\_app\_pagar      <- binding key 'app.#'

&#x20; queue q\_semua\_error    <- binding key '#.error'

&#x20; queue q\_long\_apa\_saja  <- binding key '\*.long'



Pesan dikirim:

&#x20; publish routing\_key='app.long'

&#x20; publish routing\_key='app.short'

&#x20; publish routing\_key='app.long.urgent'

&#x20; publish routing\_key='app'

&#x20; publish routing\_key='log.error'

&#x20; publish routing\_key='db.query.error'

&#x20; publish routing\_key='web.long'



Hasil (isi tiap queue):

&#x20; q\_app\_bintang    menerima 2: \['app.long', 'app.short']

&#x20; q\_app\_pagar      menerima 4: \['app.long', 'app.short', 'app.long.urgent', 'app']

&#x20; q\_semua\_error    menerima 2: \['log.error', 'db.query.error']

&#x20; q\_long\_apa\_saja  menerima 2: \['app.long', 'web.long']

PS C:\\Users\\ignio\\Documents\\celery-uts> python producer.py

\[Producer] Kirim : REQ 1 req1

\[Producer] Kirim : REQ 2 req2

\[Producer] Kirim : REQ 1 req3

\[Producer] Kirim : REQ 2 req4

\[Producer] Kirim : REQ 1 req5

\[Producer] Kirim : REQ 2 req6

\[Producer] Balasan: RES w1 1 hasil komputasi kompleks untuk 'req1'

\[Producer] Balasan: RES w1 2 echo: req2

\[Producer] Balasan: RES w1 1 hasil komputasi kompleks untuk 'req3'

\[Producer] Balasan: RES w1 2 echo: req4

\[Producer] Balasan: RES w1 1 hasil komputasi kompleks untuk 'req5'

\[Producer] Balasan: RES w1 2 echo: req6



\[Producer] Selesai dalam 9.4 detik

\[Producer] Total per worker: {'w1': 6}```

### Analisis

* Task **Long** (3 detik) tidak menghalangi task **Short** karena worker lain tetap mengambil
pesan dari queue; dengan `prefetch\_multiplier=1` pesan tidak menumpuk pada worker yang sibuk.
* Berbeda dengan UTS, kegagalan worker ditangani broker: pesan tanpa `ack` (karena `acks\_late`)
dikembalikan ke queue dan dikerjakan worker lain.
* Pada UTS, logika `rr`/`least` ditulis manual dengan `lock`; di Celery logika itu diganti
mekanisme konsumsi RabbitMQ sehingga tidak ada race condition pada kode aplikasi.

## 6\. Struktur Repositori

```
celery\_app.py      konfigurasi Celery, broker, queue, routing
tasks.py           long\_task (App 1) dan short\_task (App 2)
producer.py        input generator
exchanges/messaging\_demo.py   demo fan-out, topic, wildcard
docker-compose.yml RabbitMQ
requirements.txt
```

