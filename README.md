# Implementasi Studi Kasus Jakarta Smart City (JSC)

Mata Kuliah Komputasi Paralel dan Terdistribusi

Simulasi Komputasi Paralel dan Sistem Terdistribusi pada Sistem
Pemrosesan Data Terpadu Jakarta Smart City — integrasi laporan warga
(JAKI) dan pemantauan IoT CCTV/sensor lalu lintas.

## Struktur File

| File                               | Peran pada Arsitektur                                                            |
| ---------------------------------- | -------------------------------------------------------------------------------- |
| `main_server.py`                   | Main Server pusat data — TCP server + distributed DB + heartbeat + status export |
| `api_gateway.py`                   | API Gateway HTTP/REST — routing dan load balancing ke edge node wilayah          |
| `edge_node.py`                     | Edge Node wilayah — TCP server untuk client + proses async + sync ke Main Server |
| `client_simulator.py`              | Simulasi aplikasi warga JAKI & sensor CCTV (pembangkit traffic)                  |
| `benchmark_kinerja.py`             | Analisis Kinerja: Sequential vs Async                                            |
| `dashboard_server.py`              | HTTP server untuk Dashboard Monitoring                                           |
| `dashboard.html`                   | Web UI monitoring real-time (command center)                                     |
| `simulasi_fault.py`                | Simulasi otomatis 3 skenario fault tolerance                                     |
| `simulasi_skala_penuh.py`          | Simulasi otomatis dalam skala penuh                                              |
| `jsc_distributed_architecture.png` | Diagram arsitektur sistem                                                        |

## Kebutuhan

- **Python 3.8+** — tidak ada library eksternal (hanya modul bawaan)
- **Browser modern** — untuk dashboard (Chrome/Edge/Firefox)
- **Koneksi internet** — hanya untuk memuat Chart.js CDN di dashboard

## Alur Simulasi dan Pengujian Sistem

Proyek ini dilengkapi dengan berbagai skenario simulasi dan pengujian untuk membuktikan efektivitas dari konsep sistem terdistribusi dan komputasi paralel yang diterapkan. Berikut adalah alurnya:

1. **Simulasi Skala Penuh (`simulasi_skala_penuh.py`)**: Digunakan untuk mendemokan arsitektur secara keseluruhan (End-to-End). Skrip ini menghidupkan seluruh komponen mulai dari Main Server, API Gateway, 5 Edge Node untuk kelima wilayah Jakarta, hingga Dashboard, serta membombardir sistem dengan *traffic* laporan secara konstan.
   - **Kegunaan:** Membuktikan bahwa sistem mampu berjalan terintegrasi dan menangani request berskala besar lintas wilayah secara real-time.
2. **Pengujian Kinerja / Benchmark (`benchmark_kinerja.py`)**: Digunakan untuk membandingkan kecepatan pemrosesan data antara pendekatan tradisional (Sequential/Blocking) melawan pendekatan Asynchronous (Non-blocking).
   - **Kegunaan:** Membuktikan pemenuhan analisis kinerja bahwa komputasi asinkron jauh lebih efisien untuk operasi I/O-bound.
3. **Simulasi Toleransi Kesalahan (`simulasi_fault.py`)**: Skrip pengujian terisolasi yang mensimulasikan kegagalan pada jaringan atau server (Client crash, Server down, Node mati).
   - **Kegunaan:** Membuktikan pemenuhan tantangan sistem bahwa sistem memiliki *fault tolerance* dan jaminan konsistensi (Vector Clock).

## Cara Menjalankan

### A. Demo Skala Penuh

Cara paling mudah untuk mendemokan sistem ini adalah menggunakan script simulasi skala penuh. Script ini secara otomatis menjalankan Main Server, API Gateway, Dashboard, kelima Edge Node, dan terus-menerus menembakkan data acak ke semua node secara _background_.

Cukup buka 1 terminal dan jalankan:

```
python simulasi_skala_penuh.py
```

Lalu buka browser Anda ke `http://localhost:8080`.
Anda akan melihat dashboard command center yang menyala secara dinamis, dengan data yang masuk terus-menerus dari seluruh Jakarta. Tekan `Ctrl+C` di terminal jika ingin mematikan semua layanan.

### B. Demo Manual (Sistem Lengkap)

Jika Anda ingin menjalankan komponen satu-per-satu secara manual, buka **6 terminal** terpisah dan jalankan dalam urutan berikut:

**Terminal 1 — Main Server:**
```
python main_server.py --port 9000
```

**Terminal 2 — API Gateway:**
```
python api_gateway.py --port 8000
```

**Terminal 3 — Dashboard Server:**
```
python dashboard_server.py --port 8080
```
Buka browser → `http://localhost:8080`

**Terminal 4 & 5 — Edge Node per wilayah:**
```
python edge_node.py --region Jaksel --port 9001
python edge_node.py --region Jakpus --port 9002
```

**Terminal 6 — Simulasi client (kirim data dummy):**
```
python client_simulator.py --port 8000 --region Jaksel --jumlah 50
python client_simulator.py --port 8000 --region Jakpus --jumlah 50
```
Tunggu 5 detik → lihat sync terjadi di terminal Edge Node + data muncul di dashboard.

**Mode Chaos (simulasi client crash):**
```
python client_simulator.py --port 8000 --region Jaksel --jumlah 50 --chaos
```

### C. Benchmark Kinerja (End-to-End)

Script benchmark secara otomatis menjalankan dua server TCP yang berbeda arsitektur (sequential dan asynchronous) sebagai subprocess, lalu membombardir keduanya dengan volume request yang identik dari client konkuren melalui koneksi TCP nyata.

Cukup jalankan satu perintah (server otomatis dinyalakan dan dimatikan oleh script):

```
python benchmark_kinerja.py --jumlah 100
python benchmark_kinerja.py --jumlah 1000
python benchmark_kinerja.py --jumlah 10000
```

> **Catatan:** Untuk `--jumlah 10000` pada server sequential, perlu waktu ~5 menit karena server memproses satu request per waktu.

### D. Simulasi Fault Tolerance

Cukup jalankan satu perintah (semua proses otomatis dikelola script):

```
python simulasi_fault.py
```

Menjalankan 3 skenario otomatis:
1. **Client crash** di tengah pengiriman → Edge Node tetap melayani
2. **Main Server down** → data di-buffer → recovery otomatis saat server pulih
3. **Edge Node offline** → Main Server deteksi via heartbeat monitoring

## Pemetaan ke Poin Tugas

### Asynchronous Programming
**Proses:** Pengolahan data pengguna (laporan warga JAKI) + data sensor CCTV.

| Fungsi             | File           | Penjelasan                                                               |
| ------------------ | -------------- | ------------------------------------------------------------------------ |
| `proses_laporan()` | `edge_node.py` | Validasi & simpan data dengan `await asyncio.sleep()` — non-blocking I/O |
| `handle_client()`  | `edge_node.py` | Setiap koneksi = coroutine independen, banyak client dilayani bersamaan  |

### Sistem Terdistribusi (2 implementasi)

**5.1 Socket Programming (Client-Server):**
- `api_gateway.py` → menerima request HTTP dari aplikasi warga / CCTV
- `edge_node.py` → `asyncio.start_server()` melayani request yang diteruskan gateway
- `main_server.py` → `asyncio.start_server()` melayani Edge Node

**5.1a API Gateway:**
- `client_simulator.py` → mengirim request HTTP ke `api_gateway.py`
- `api_gateway.py` → merutekan request ke edge node yang sesuai berdasarkan wilayah
- `api_gateway.py` → menerapkan routing dan load balancing sederhana via round-robin

**5.2 Komunikasi Antar Node:**
- `edge_node.py` → `sync_ke_main_server()` mengirim rekap data setiap N detik ke Main Server via koneksi socket terpisah
- Data disertai **Vector Clock** untuk menjaga urutan kausal antar-node

### Analisis Kinerja

**File:** `benchmark_kinerja.py`

Benchmark End-to-End yang membandingkan dua arsitektur server TCP secara langsung:

- **Server Sequential (Blocking):** Menggunakan `socket.accept()` + `time.sleep()`. Server hanya mampu melayani satu request pada satu waktu. Selama memproses satu request, semua request lain harus mengantri.
- **Server Asynchronous (Non-blocking):** Menggunakan `asyncio.start_server()` + `asyncio.sleep()`. Server mampu melayani banyak request secara bersamaan (konkuren). Saat satu request menunggu I/O, event loop langsung beralih melayani request lain.

Kedua server menggunakan simulasi delay I/O yang identik (0.01s — 0.05s) dan menerima traffic dari kode client yang sama (hingga 50 koneksi simultan melalui koneksi TCP nyata).

Hasil pengujian (N=100): Server async mencapai **speedup ~24.6x** dibandingkan server sequential. Speedup akan meningkat seiring jumlah data karena server async memanfaatkan konkuren secara optimal.

Rumus: `Speedup = T_sequential / T_async`

### Tantangan Sistem & Solusi

| Tantangan                             | Implementasi                                                                   | File                                                    |
| ------------------------------------- | ------------------------------------------------------------------------------ | ------------------------------------------------------- |
| **Fault Tolerance**                   | Buffer persistence + exponential backoff (1s→2s→4s→...→30s) saat sync gagal    | `edge_node.py`                                          |
| **Konsistensi Data (Event Ordering)** | Vector Clock — setiap pesan menyertakan VC, merge pada penerima                | `edge_node.py`, `main_server.py`, `client_simulator.py` |
| **Graceful Client Disconnect**        | Tangkap semua exception jaringan, satu client crash tidak mengganggu yang lain | `edge_node.py`                                          |
| **Heartbeat Monitoring**              | Main Server memantau last_sync tiap 10 detik, tandai OFFLINE jika > 30 detik   | `main_server.py`                                        |
| **Latency Jaringan**                  | Timeout pada semua operasi socket (10 detik) + retry otomatis                  | `edge_node.py`                                          |

## Fitur Vector Clock

Setiap node memiliki **Vector Clock** — dictionary `{node_id: counter}`:
- Sebelum kirim pesan: increment counter sendiri
- Saat menerima pesan: merge VC (`max` per key)
- Main Server menyimpan VC global = merge dari semua Edge Node

Dashboard menampilkan matriks Vector Clock secara visual.

## Fitur Fault Tolerance

- Kegagalan koneksi satu client **tidak** mengganggu client lain
- Data yang gagal sync **dikembalikan ke buffer**, tidak hilang
- Retry dengan **exponential backoff** agar tidak membombardir server
- Main Server mendeteksi node mati via **heartbeat** dan mencatat alert
- Semua status tersedia di **Dashboard** secara real-time

## Dashboard Monitoring

Dashboard web bergaya **command center** smart city:
- Peta Jakarta dengan indikator status 5 node wilayah (hijau/merah)
- Grafik throughput real-time (Chart.js)
- Status panel per Edge Node + Vector Clock visualizer
- Tabel data terbaru + Alert log fault tolerance
- Auto-refresh setiap 2 detik

## Argumen CLI

### main_server.py
| Argumen  | Default | Keterangan                                  |
| -------- | ------- | ------------------------------------------- |
| `--port` | 9000    | Port TCP untuk menerima sync dari Edge Node |

### api_gateway.py
| Argumen  | Default | Keterangan                                  |
| -------- | ------- | ------------------------------------------- |
| `--port` | 8000    | Port HTTP untuk menerima request client     |

### edge_node.py
| Argumen           | Default   | Keterangan                                   |
| ----------------- | --------- | -------------------------------------------- |
| `--region`        | (wajib)   | Nama wilayah, contoh: Jaksel                 |
| `--port`          | (wajib)   | Port TCP untuk menerima data client          |
| `--main-host`     | 127.0.0.1 | Host Main Server                             |
| `--main-port`     | 9000      | Port Main Server                             |
| `--sync-interval` | 5         | Interval sinkronisasi ke Main Server (detik) |

### client_simulator.py
| Argumen    | Default   | Keterangan                            |
| ---------- | --------- | ------------------------------------- |
| `--host`   | 127.0.0.1 | Host API Gateway tujuan               |
| `--port`   | (wajib)   | Port API Gateway tujuan               |
| `--region` | (wajib)   | Nama wilayah tujuan                   |
| `--jumlah` | 50        | Jumlah data dummy yang dikirim        |
| `--chaos`  | False     | Mode chaos: 10% koneksi diputus paksa |

### benchmark_kinerja.py
| Argumen    | Default | Keterangan              |
| ---------- | ------- | ----------------------- |
| `--jumlah` | 100     | Jumlah data untuk diuji |

### dashboard_server.py
| Argumen  | Default | Keterangan                 |
| -------- | ------- | -------------------------- |
| `--port` | 8080    | Port HTTP server dashboard |
