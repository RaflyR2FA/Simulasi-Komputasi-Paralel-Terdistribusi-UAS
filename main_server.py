"""
main_server.py
==============
Main Server (Pusat Data) - Jakarta Smart City (JSC)

Implementasi yang dibuktikan oleh file ini:

  - Poin 5.1 (Sistem Terdistribusi - Socket Programming Client-Server)
        -> handle_edge_node() melalui asyncio.start_server()
        Main Server berperan sebagai TCP server yang menerima koneksi
        dari banyak Edge Node sekaligus.

  - Poin 5.2 (Sistem Terdistribusi - Komunikasi Antar Node)
        -> menerima paket rekapitulasi terjadwal dari tiap Edge Node
        (lihat edge_node.py -> sync_ke_main_server)

  - Simulasi Distributed Database (mendukung rancangan arsitektur poin 3):
        -> simpan_ke_distributed_db()
        Data dipisah ke 'cluster' file berbeda sesuai jenisnya - meniru
        pemisahan node penyimpanan laporan warga vs log sensor.

  - Poin 6 (Konsistensi Data - Vector Clock):
        -> VectorClock class
        Menjaga urutan logis kejadian di seluruh node dalam sistem
        terdistribusi. Setiap sinkronisasi dari Edge Node akan
        meng-increment dan merge vector clock global.

  - Poin 7 (Fault Tolerance - Monitoring & Alerting):
        -> monitor_edge_nodes() coroutine
        Memantau status edge node setiap 10 detik. Jika sebuah node
        tidak melakukan sinkronisasi selama > 30 detik, statusnya
        akan diubah menjadi OFFLINE dan alert WARNING dicatat.

        -> tulis_status_json() coroutine
        Menulis file status.json setiap 3 detik agar dashboard web
        dapat membaca state terkini dari sistem secara real-time.

Cara menjalankan:
    python main_server.py --port 9000
"""

import asyncio
import json
import os
import argparse
from datetime import datetime


# =============================================================================
# Vector Clock — Menjaga Konsistensi & Urutan Logis Kejadian (Poin 6)
# =============================================================================
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


# =============================================================================
# Konstanta & Variabel Global
# =============================================================================

DB_LAPORAN = "db_laporan_warga.json"
DB_SENSOR = "db_log_sensor.json"

# Variabel-variabel berikut membutuhkan event loop yang aktif, sehingga
# diinisialisasi di dalam main() menggunakan deklarasi 'global'.
db_lock = None          # asyncio.Lock — melindungi akses file DB
registry_lock = None    # asyncio.Lock — melindungi akses node_registry

vc = None               # VectorClock — vector clock global Main Server
node_registry = None    # dict — status tiap Edge Node yang terdaftar
alert_log = None        # list — log alert/peringatan sistem
stats = None            # dict — statistik agregat (total laporan, sensor, sync)
throughput_history = None  # list — riwayat throughput untuk grafik dashboard
start_time = None       # datetime — waktu server pertama kali aktif


# =============================================================================
# Helper: Load & Save JSON (Distributed Database Files)
# =============================================================================

def _load(path: str) -> list:
    """Muat data dari file JSON. Kembalikan list kosong jika file tidak ada atau rusak."""
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []
    return []


def _save(path: str, data: list):
    """Simpan data ke file JSON dengan indentasi untuk keterbacaan."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# =============================================================================
# Distributed Database — Simpan Data Laporan & Sensor (Poin 3)
# =============================================================================

async def simpan_ke_distributed_db(paket: dict) -> tuple:
    """
    Simulasi Distributed Database: data laporan warga & log sensor ditulis
    ke FILE / 'cluster' TERPISAH, merepresentasikan pemisahan node
    penyimpanan sesuai rancangan arsitektur poin 3.

    Selain menyimpan ke file, fungsi ini juga memperbarui statistik global
    (stats) agar dashboard selalu menampilkan data terkini.
    """
    global stats

    async with db_lock:
        db_laporan = _load(DB_LAPORAN)
        db_sensor = _load(DB_SENSOR)

        n_laporan_baru = len(paket.get("laporan_warga", []))
        n_sensor_baru = len(paket.get("log_sensor", []))

        db_laporan.extend(paket.get("laporan_warga", []))
        db_sensor.extend(paket.get("log_sensor", []))

        _save(DB_LAPORAN, db_laporan)
        _save(DB_SENSOR, db_sensor)

        # Update statistik global
        stats["total_laporan"] = len(db_laporan)
        stats["total_sensor"] = len(db_sensor)
        stats["total_sync"] += 1

        return len(db_laporan), len(db_sensor)


# =============================================================================
# Handle Edge Node — Penerima Sinkronisasi (Poin 5.1 & 5.2 & 6)
# =============================================================================

async def handle_edge_node(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """
    [Poin 5.1 & 5.2 & Poin 6 - Vector Clock]
    Menangani satu koneksi sinkronisasi masuk dari sebuah Edge Node.
    Beberapa Edge Node (Jakpus, Jakut, Jakbar, Jaksel, Jaktim) dapat
    melakukan sync ke Main Server ini secara bersamaan; asyncio.start_server
    menangani tiap koneksi tersebut secara konkuren (non-blocking).

    Alur Vector Clock pada setiap sync:
      1. Increment VC lokal (Main Server menerima event baru)
      2. Merge VC dari Edge Node (sinkronisasi urutan kausal)
      3. Kirim VC terbaru kembali ke Edge Node dalam respons

    Fault Tolerance:
      - Seluruh proses dibungkus try/except agar satu koneksi yang gagal
        tidak menghentikan server secara keseluruhan.
      - Error dicatat ke alert_log untuk monitoring di dashboard.
    """
    global vc, node_registry, alert_log

    addr = writer.get_extra_info("peername")
    try:
        raw = await reader.readline()
        if not raw:
            return

        paket = json.loads(raw.decode().strip())
        region = paket.get("region", "Unknown")

        # --- Vector Clock: increment & merge ---
        incoming_vc = paket.get("vector_clock", {})
        vc.increment()
        vc.merge(incoming_vc)

        # --- Simpan ke Distributed DB ---
        total_laporan, total_sensor = await simpan_ke_distributed_db(paket)

        n_laporan = len(paket.get("laporan_warga", []))
        n_sensor = len(paket.get("log_sensor", []))

        # --- Update Node Registry ---
        async with registry_lock:
            if region not in node_registry:
                node_registry[region] = {
                    "status": "ONLINE",
                    "last_sync": datetime.now(),
                    "total_data": 0,
                    "vector_clock": {},
                    "total_sync": 0,
                    "sync_gagal": 0,
                }
            node_registry[region]["status"] = "ONLINE"
            node_registry[region]["last_sync"] = datetime.now()
            node_registry[region]["total_data"] += (n_laporan + n_sensor)
            node_registry[region]["vector_clock"] = incoming_vc
            node_registry[region]["total_sync"] += 1

        print(f"[MainServer] << SYNC diterima dari Edge Node '{region}' ({addr}): "
              f"+{n_laporan} laporan warga, +{n_sensor} log sensor "
              f"| total Distributed DB sekarang -> laporan={total_laporan}, sensor={total_sensor} "
              f"| VC={vc}")

        # --- Kirim balasan dengan Vector Clock ---
        balasan = json.dumps({
            "status": "diterima",
            "waktu_server": datetime.now().isoformat(timespec="seconds"),
            "total_laporan_db": total_laporan,
            "total_sensor_db": total_sensor,
            "vector_clock": vc.to_dict(),
        }) + "\n"
        writer.write(balasan.encode())
        await writer.drain()

    except Exception as e:
        print(f"[MainServer] ERROR menangani koneksi dari {addr}: {e}")
        # Catat alert agar terlihat di dashboard
        alert_log.append({
            "waktu": datetime.now().isoformat(timespec="seconds"),
            "level": "ERROR",
            "pesan": f"Gagal memproses sync dari {addr}: {e}",
        })

    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# =============================================================================
# Monitor Edge Nodes — Deteksi Node Offline (Poin 7 - Fault Tolerance)
# =============================================================================

async def monitor_edge_nodes():
    """
    [Poin 7 - Fault Tolerance]
    Coroutine background yang berjalan setiap 10 detik.
    Memeriksa setiap node di registry: jika sudah > 30 detik sejak
    sinkronisasi terakhir dan status masih ONLINE, maka:
      - Status diubah menjadi OFFLINE
      - Alert WARNING ditambahkan ke alert_log
      - Peringatan dicetak ke console

    Ini merupakan mekanisme deteksi kegagalan sederhana (failure detector)
    yang umum digunakan dalam sistem terdistribusi.
    """
    global node_registry, alert_log

    while True:
        await asyncio.sleep(10)

        now = datetime.now()
        async with registry_lock:
            for region, info in node_registry.items():
                last_sync = info.get("last_sync")
                if last_sync and (now - last_sync).total_seconds() > 30:
                    if info["status"] == "ONLINE":
                        info["status"] = "OFFLINE"
                        info["sync_gagal"] = info.get("sync_gagal", 0) + 1
                        pesan = f"Edge Node {region} OFFLINE (tidak sync > 30 detik)"
                        alert_log.append({
                            "waktu": now.isoformat(timespec="seconds"),
                            "level": "WARNING",
                            "pesan": pesan,
                        })
                        print(f"[MainServer] ⚠ WARNING: {pesan}")


# =============================================================================
# Tulis status.json — Interface ke Dashboard (Poin 7 - Observability)
# =============================================================================

async def tulis_status_json():
    """
    [Poin 7 - Fault Tolerance & Observability]
    Coroutine background yang menulis file status.json setiap 3 detik.
    File ini dibaca oleh dashboard web untuk menampilkan state terkini
    dari seluruh sistem secara real-time.

    Format output mengikuti kontrak yang disepakati dengan komponen
    dashboard (lihat dokumentasi format di bagian atas file ini).

    Throughput history dicatat untuk menampilkan grafik tren waktu.
    Hanya 60 entry terakhir yang disimpan (≈ 3 menit data).
    """
    global throughput_history

    while True:
        await asyncio.sleep(3)

        now = datetime.now()
        uptime = (now - start_time).total_seconds()

        # Catat titik throughput baru
        throughput_entry = {
            "waktu": now.isoformat(timespec="seconds"),
            "laporan": stats["total_laporan"],
            "sensor": stats["total_sensor"],
        }
        throughput_history.append(throughput_entry)
        # Batasi riwayat agar tidak membengkak (simpan 60 entry terakhir)
        if len(throughput_history) > 60:
            throughput_history[:] = throughput_history[-60:]

        # Buat snapshot node_registry yang JSON-serializable
        async with registry_lock:
            registry_snapshot = {}
            for region, info in node_registry.items():
                registry_snapshot[region] = {
                    "status": info["status"],
                    "last_sync": info["last_sync"].isoformat(timespec="seconds") if info.get("last_sync") else None,
                    "total_data": info.get("total_data", 0),
                    "vector_clock": info.get("vector_clock", {}),
                    "total_sync": info.get("total_sync", 0),
                    "sync_gagal": info.get("sync_gagal", 0),
                }

        status = {
            "timestamp": now.isoformat(timespec="seconds"),
            "uptime_seconds": int(uptime),
            "vector_clock_global": vc.to_dict(),
            "node_registry": registry_snapshot,
            "stats": dict(stats),
            "alerts": list(alert_log[-50:]),  # simpan 50 alert terakhir
            "throughput_history": list(throughput_history),
        }

        try:
            with open("status.json", "w", encoding="utf-8") as f:
                json.dump(status, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"[MainServer] Gagal menulis status.json: {e}")


# =============================================================================
# Main — Entry Point
# =============================================================================

async def main(port: int):
    """
    Fungsi utama Main Server.
    Menginisialisasi semua state global, menjalankan background tasks
    (monitoring & status writer), dan memulai TCP server.
    """
    global db_lock, registry_lock, vc, node_registry, alert_log
    global stats, throughput_history, start_time

    # Inisialisasi semua state global di dalam event loop
    db_lock = asyncio.Lock()
    registry_lock = asyncio.Lock()
    vc = VectorClock("MainServer")
    node_registry = {}
    alert_log = []
    stats = {"total_laporan": 0, "total_sensor": 0, "total_sync": 0}
    throughput_history = []
    start_time = datetime.now()

    # Muat data DB yang sudah ada untuk menginisialisasi stats
    existing_laporan = _load(DB_LAPORAN)
    existing_sensor = _load(DB_SENSOR)
    stats["total_laporan"] = len(existing_laporan)
    stats["total_sensor"] = len(existing_sensor)

    # Jalankan background tasks
    asyncio.create_task(monitor_edge_nodes())
    asyncio.create_task(tulis_status_json())

    server = await asyncio.start_server(handle_edge_node, "0.0.0.0", port)

    print("=" * 70)
    print("  Jakarta Smart City — Main Server (Pusat Data)")
    print("=" * 70)
    print(f"  Port              : {port}")
    print(f"  Distributed DB    : {DB_LAPORAN} & {DB_SENSOR}")
    print(f"  Data existing     : {stats['total_laporan']} laporan, {stats['total_sensor']} sensor")
    print(f"  Vector Clock      : {vc}")
    print(f"  Status file       : status.json (update setiap 3 detik)")
    print(f"  Node monitoring   : setiap 10 detik (timeout 30 detik)")
    print(f"  Waktu start       : {start_time.isoformat(timespec='seconds')}")
    print("=" * 70)
    print()

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Main Server - Jakarta Smart City")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()

    try:
        asyncio.run(main(args.port))
    except KeyboardInterrupt:
        print("\n[MainServer] dihentikan oleh pengguna.")
