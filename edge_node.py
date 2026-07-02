"""
edge_node.py
============
Edge Node wilayah - Jakarta Smart City (JSC)

Merepresentasikan satu node wilayah (mis. Jaksel, Jakpus, dst.) pada
arsitektur sistem terdistribusi yang sudah dirancang di poin 3.

Implementasi yang dibuktikan oleh file ini:

  - Poin 4 (Komputasi Paralel - Asynchronous Programming)
        -> proses_laporan() & handle_client()
        Memproses banyak laporan warga / data sensor secara bersamaan
        (non-blocking), tanpa menunggu satu laporan selesai diproses
        sebelum melayani laporan berikutnya.

  - Poin 5.1 (Sistem Terdistribusi - Socket Programming Client-Server)
        -> handle_client() melalui asyncio.start_server()
        Edge Node berperan sebagai TCP server yang melayani client
        (aplikasi warga JAKI & sensor CCTV).

  - Poin 5.2 (Sistem Terdistribusi - Komunikasi Antar Node)
        -> sync_ke_main_server()
        Edge Node berperan sebagai TCP client yang mengirim rekap data
        ke Main Server setiap N detik.

  - Poin 6 (Konsistensi Data - Vector Clock):
        -> VectorClock class
        Setiap data yang diproses dan setiap sinkronisasi ke Main Server
        di-tag dengan vector clock, memungkinkan urutan kausal event
        ditentukan secara konsisten di seluruh sistem terdistribusi.

  - Poin 7 (Fault Tolerance - Exponential Backoff & Error Recovery):
        -> sync_ke_main_server() dengan exponential backoff
        Jika Main Server tidak dapat dihubungi, Edge Node tidak langsung
        menyerah. Ia mengembalikan data ke buffer dan mencoba lagi
        dengan interval yang bertambah secara eksponensial (1s, 2s, 4s,
        ..., max 30s). Ketika koneksi berhasil, interval direset ke 1s.

        -> handle_client() dengan comprehensive error handling
        Menangkap semua jenis kegagalan koneksi (ConnectionReset,
        BrokenPipe, Timeout, dll.) agar satu client yang bermasalah
        tidak menghentikan layanan untuk client lainnya.

        -> cetak_statistik() coroutine
        Mencetak ringkasan statistik setiap 30 detik untuk monitoring.

Cara menjalankan:
    python edge_node.py --region Jaksel --port 9001
"""

import asyncio
import json
import random
import argparse
from datetime import datetime


class VectorClock:
    """
    Implementasi Vector Clock untuk menentukan urutan kausal (causal ordering)
    kejadian di dalam sistem terdistribusi.

    Setiap node menyimpan counter untuk dirinya sendiri DAN untuk setiap node
    lain yang pernah berkomunikasi dengannya. Dengan membandingkan vector clock
    dua kejadian, kita dapat menentukan apakah satu kejadian 'terjadi sebelum'
    yang lain, atau keduanya terjadi secara konkuren.
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.clock = {}

    def increment(self):
        """Increment counter untuk node ini sendiri (terjadi sebelum setiap event lokal)."""
        self.clock[self.node_id] = self.clock.get(self.node_id, 0) + 1

    def merge(self, other_clock: dict):
        """
        Merge vector clock dari node lain (dilakukan saat menerima pesan).
        Untuk setiap entry, ambil nilai maksimum antara clock lokal dan
        clock yang diterima — ini menjamin bahwa urutan kausal terjaga.
        """
        for node, count in other_clock.items():
            self.clock[node] = max(self.clock.get(node, 0), count)

    def to_dict(self) -> dict:
        """Kembalikan salinan vector clock sebagai dict biasa (untuk serialisasi JSON)."""
        return dict(self.clock)

    def __str__(self):
        return str(self.clock)

buffer_laporan = []
buffer_sensor = []
lock = None
REGION = None

vc = None
stats = None

async def proses_laporan(data: dict) -> dict:
    """
    [Poin 4 - Asynchronous Programming]
    Memvalidasi & 'menyimpan' satu data masuk (laporan warga ATAU log sensor).

    Operasi I/O seperti menyimpan foto ke storage / insert ke database
    disimulasikan dengan asyncio.sleep(). Karena memakai 'await', event loop
    TIDAK diam menunggu - ia bebas berpindah memproses koneksi client lain
    yang masuk bersamaan. Inilah yang membedakan pendekatan ini dari
    pemrosesan sequential (satu per satu, baris demi baris, blocking).

    Vector Clock: setiap data yang diproses di-increment VC-nya, sehingga
    urutan pemrosesan dapat dilacak secara kausal di seluruh sistem.
    """
    global vc

    vc.increment()

    delay_io = random.uniform(0.05, 0.25)
    await asyncio.sleep(delay_io)

    data["status_validasi"] = "valid"
    data["diproses_oleh"] = f"EdgeNode-{REGION}"
    data["waktu_proses"] = datetime.now().isoformat(timespec="milliseconds")
    data["vector_clock"] = vc.to_dict()
    return data

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """
    [Poin 5.1 - Socket Programming Client-Server & Poin 7 - Fault Tolerance]
    Menangani satu koneksi socket dari client (aplikasi warga / sensor CCTV).
    asyncio.start_server() memanggil fungsi ini setiap kali ada client baru
    yang terhubung, dan banyak koneksi dapat ditangani BERSAMAAN karena
    sifat asinkron dari coroutine ini (lihat proses_laporan di atas).

    Fault Tolerance:
      - Menangkap semua jenis kegagalan koneksi (ConnectionReset, BrokenPipe,
        IncompleteRead, Timeout, dll.) agar satu client bermasalah tidak
        menghentikan layanan untuk client lainnya.
      - Statistik koneksi sukses/gagal dicatat untuk monitoring.

    Vector Clock:
      - Jika client mengirim data dengan vector_clock, VC lokal akan di-merge
        untuk menjaga konsistensi kausal antar komponen.
    """
    global vc, stats

    addr = writer.get_extra_info("peername")
    try:
        raw = await reader.readline()
        if not raw:
            return

        try:
            data = json.loads(raw.decode().strip())
        except json.JSONDecodeError:
            writer.write(b'{"status":"error","pesan":"format JSON tidak valid"}\n')
            await writer.drain()
            stats["koneksi_gagal"] += 1
            return

        if "vector_clock" in data:
            vc.merge(data["vector_clock"])

        hasil = await proses_laporan(data)

        async with lock:
            if hasil.get("tipe") == "log_sensor":
                buffer_sensor.append(hasil)
            else:
                buffer_laporan.append(hasil)
            n_laporan, n_sensor = len(buffer_laporan), len(buffer_sensor)

        stats["koneksi_sukses"] += 1
        stats["total_diproses"] += 1

        id_data = hasil.get("id_user") or hasil.get("id_cctv") or "?"
        print(f"[EdgeNode-{REGION}] diterima dari {addr} | tipe={hasil.get('tipe')} "
              f"id={id_data} | buffer saat ini: laporan={n_laporan}, sensor={n_sensor}")

        ack = json.dumps({
            "status": "diterima",
            "diproses_oleh": f"EdgeNode-{REGION}",
            "vector_clock": vc.to_dict(),
        }) + "\n"
        writer.write(ack.encode())
        await writer.drain()

    except (ConnectionResetError, asyncio.IncompleteReadError, BrokenPipeError,
            OSError, asyncio.TimeoutError) as e:
        stats["koneksi_gagal"] += 1
        print(f"[EdgeNode-{REGION}] koneksi error dari {addr}: {type(e).__name__}: {e}")

    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

async def sync_ke_main_server(main_host: str, main_port: int, interval: int):
    """
    [Poin 5.2 - Komunikasi Antar Node & Poin 6 - Vector Clock & Poin 7 - Fault Tolerance]
    Setiap `interval` detik, Edge Node membungkus seluruh data yang
    terkumpul di buffer menjadi satu paket rekapitulasi, lalu mengirimkannya
    ke Main Server lewat koneksi socket TERPISAH (Edge Node berperan
    sebagai client di sini, kebalikan dari perannya di handle_client).

    Vector Clock:
      - VC di-increment sebelum mengirim (event pengiriman)
      - VC dari respons Main Server di-merge (sinkronisasi kausal)

    Fault Tolerance — Exponential Backoff:
      - Jika koneksi ke Main Server gagal, data dikembalikan ke buffer
        agar TIDAK HILANG.
      - Interval retry bertambah secara eksponensial: 1s -> 2s -> 4s -> ... -> 30s (max)
      - Ketika koneksi berhasil, interval direset kembali ke 1s (base).
      - Ini mencegah 'thundering herd' saat Main Server kembali online
        setelah downtime yang lama.
    """
    global vc, stats

    backoff = 1

    while True:
        await asyncio.sleep(interval)

        async with lock:
            if not buffer_laporan and not buffer_sensor:
                continue
            vc.increment()

            paket = {
                "region": REGION,
                "waktu_sync": datetime.now().isoformat(timespec="seconds"),
                "laporan_warga": buffer_laporan.copy(),
                "log_sensor": buffer_sensor.copy(),
                "vector_clock": vc.to_dict(),
            }
            jumlah_laporan = len(buffer_laporan)
            jumlah_sensor = len(buffer_sensor)
            buffer_laporan.clear()
            buffer_sensor.clear()

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(main_host, main_port),
                timeout=10
            )
            writer.write((json.dumps(paket) + "\n").encode())
            await writer.drain()

            balasan_raw = await asyncio.wait_for(reader.readline(), timeout=10)
            balasan = json.loads(balasan_raw.decode().strip())

            writer.close()
            await writer.wait_closed()

            if "vector_clock" in balasan:
                vc.merge(balasan["vector_clock"])

            backoff = 1
            stats["total_sync_sukses"] += 1

            print(f"[EdgeNode-{REGION}] >> SYNC ke Main Server: "
                  f"{jumlah_laporan} laporan warga + {jumlah_sensor} log sensor terkirim "
                  f"| total di Main Server sekarang: laporan={balasan.get('total_laporan_db')}, "
                  f"sensor={balasan.get('total_sensor_db')} "
                  f"| VC={vc}")

        except Exception as e:
            stats["total_sync_gagal"] += 1

            print(f"[EdgeNode-{REGION}] GAGAL sync ke Main Server "
                  f"({main_host}:{main_port}) - {type(e).__name__}: {e}. "
                  f"Data dikembalikan ke buffer. "
                  f"Retry backoff: {backoff}s (berikutnya: {min(backoff * 2, 30)}s)")

            async with lock:
                buffer_laporan.extend(paket["laporan_warga"])
                buffer_sensor.extend(paket["log_sensor"])

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

async def cetak_statistik():
    """
    [Poin 7 - Fault Tolerance & Observability]
    Coroutine background yang mencetak ringkasan statistik setiap 30 detik.
    Berguna untuk monitoring kesehatan Edge Node dari console/terminal.
    """
    while True:
        await asyncio.sleep(30)

        print()
        print(f"{'=' * 60}")
        print(f"  [EdgeNode-{REGION}] STATISTIK (setiap 30 detik)")
        print(f"{'=' * 60}")
        print(f"  Koneksi sukses    : {stats['koneksi_sukses']}")
        print(f"  Koneksi gagal     : {stats['koneksi_gagal']}")
        print(f"  Total diproses    : {stats['total_diproses']}")
        print(f"  Sync sukses       : {stats['total_sync_sukses']}")
        print(f"  Sync gagal        : {stats['total_sync_gagal']}")
        print(f"  Buffer saat ini   : {len(buffer_laporan)} laporan, {len(buffer_sensor)} sensor")
        print(f"  Vector Clock      : {vc}")
        print(f"{'=' * 60}")
        print()

async def main(region: str, port: int, main_host: str, main_port: int, sync_interval: int):
    """
    Fungsi utama Edge Node.
    Menginisialisasi semua state global, menjalankan background tasks
    (sync ke Main Server & statistik periodik), dan memulai TCP server.
    """
    global REGION, lock, vc, stats

    REGION = region
    lock = asyncio.Lock()
    vc = VectorClock(f"EdgeNode-{REGION}")
    stats = {
        "koneksi_sukses": 0,
        "koneksi_gagal": 0,
        "total_diproses": 0,
        "total_sync_sukses": 0,
        "total_sync_gagal": 0,
    }

    server = await asyncio.start_server(handle_client, "0.0.0.0", port)

    print("=" * 60)
    print(f"  Jakarta Smart City — Edge Node [{REGION}]")
    print("=" * 60)
    print(f"  Port client       : {port}")
    print(f"  Main Server       : {main_host}:{main_port}")
    print(f"  Sync interval     : {sync_interval} detik")
    print(f"  Vector Clock      : {vc}")
    print(f"  Statistik         : setiap 30 detik")
    print("=" * 60)
    print()

    asyncio.create_task(sync_ke_main_server(main_host, main_port, sync_interval))
    asyncio.create_task(cetak_statistik())

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Edge Node wilayah - Jakarta Smart City")
    parser.add_argument("--region", required=True, help="Nama wilayah, contoh: Jaksel")
    parser.add_argument("--port", type=int, required=True, help="Port socket untuk menerima data client")
    parser.add_argument("--main-host", default="127.0.0.1", help="Host Main Server")
    parser.add_argument("--main-port", type=int, default=9000, help="Port Main Server")
    parser.add_argument("--sync-interval", type=int, default=5, help="Interval sinkronisasi ke Main Server (detik)")
    args = parser.parse_args()

    try:
        asyncio.run(main(args.region, args.port, args.main_host, args.main_port, args.sync_interval))
    except KeyboardInterrupt:
        print(f"\n[EdgeNode-{args.region}] dihentikan oleh pengguna.")
