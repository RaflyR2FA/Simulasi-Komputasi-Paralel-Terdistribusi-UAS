"""
benchmark_kinerja.py
====================
Poin 6 — Analisis Kinerja: Sequential vs Asynchronous (End-to-End)
Studi Kasus: Jakarta Smart City (JSC)

Script ini melakukan benchmark NYATA (End-to-End) dengan menjalankan
dua arsitektur server TCP yang berbeda secara bergantian, lalu
membombardir keduanya dengan volume request yang identik dari
client konkuren.

Skenario 1 — SEQUENTIAL SERVER (Blocking):
  Server menggunakan socket.accept() biasa + time.sleep().
  Setiap request klien HARUS menunggu request sebelumnya selesai
  diproses sebelum bisa dilayani → bottleneck.

Skenario 2 — ASYNCHRONOUS SERVER (Non-blocking):
  Server menggunakan asyncio.start_server() + asyncio.sleep().
  Semua request klien diproses secara KONKUREN oleh event loop →
  saat satu request menunggu I/O, request lain tetap dilayani.

Kedua skenario menggunakan:
  - Simulasi delay I/O yang IDENTIK (0.01s — 0.05s per data)
  - Kode client pengirim yang IDENTIK (concurrent via asyncio)
  - Jumlah data yang IDENTIK
  - Komunikasi lewat TCP socket yang NYATA (bukan simulasi)

Cara menjalankan:
    python benchmark_kinerja.py --jumlah 100
    python benchmark_kinerja.py --jumlah 1000
    python benchmark_kinerja.py --jumlah 10000
"""

import asyncio
import json
import time
import random
import argparse
import subprocess
import sys
import os
import socket
import io

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

PYTHON = sys.executable
SCRIPT = os.path.abspath(__file__)

# Simulasi delay I/O — identik dengan yang dipakai di edge_node.py
IO_DELAY_MIN = 0.01
IO_DELAY_MAX = 0.05

# Batas koneksi simultan agar tidak membebani OS (realistis: connection pool)
MAX_CONCURRENT = 50


# =====================================================================
# IMPLEMENTASI SERVER SEQUENTIAL (Blocking / Sinkron)
# =====================================================================

def _run_sequential_server(port: int):
    """
    Server TCP SEQUENTIAL (Blocking).

    Menggunakan modul `socket` standar Python. Server ini menerima
    koneksi satu per satu secara sinkron:
      1. accept() → terima koneksi
      2. recv()   → baca data
      3. sleep()  → simulasi I/O (BLOCKING — seluruh thread terhenti)
      4. send()   → kirim respons
      5. close()  → tutup koneksi
      6. Kembali ke langkah 1

    Selama server memproses satu request (langkah 2–5), request lain
    yang masuk TIDAK BISA dilayani dan harus menunggu di antrian OS
    (backlog). Ini adalah kelemahan utama arsitektur blocking.
    """
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(('127.0.0.1', port))
    server_sock.listen(socket.SOMAXCONN)

    while True:
        conn, addr = server_sock.accept()
        try:
            data = b""
            while b"\n" not in data:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data += chunk

            if data:
                payload = json.loads(data.decode().strip())
                # ---- Simulasi I/O BLOCKING (time.sleep) ----
                # Selama sleep ini, TIDAK ADA request lain yang bisa dilayani.
                time.sleep(random.uniform(IO_DELAY_MIN, IO_DELAY_MAX))
                response = json.dumps({
                    "status": "ACK",
                    "metode": "sequential",
                    "lokasi": payload.get("lokasi", "?"),
                }) + "\n"
                conn.sendall(response.encode())
        except Exception:
            pass
        finally:
            conn.close()


# =====================================================================
# IMPLEMENTASI SERVER ASYNCHRONOUS (Non-blocking / Async)
# =====================================================================

def _run_async_server(port: int):
    """
    Server TCP ASYNCHRONOUS (Non-blocking).

    Menggunakan `asyncio.start_server()`. Setiap koneksi masuk
    ditangani oleh coroutine terpisah yang berjalan secara konkuren
    di dalam satu event loop:
      1. start_server()  → terima banyak koneksi sekaligus
      2. readline()      → baca data (non-blocking)
      3. asyncio.sleep() → simulasi I/O (NON-BLOCKING — event loop
                           beralih ke coroutine lain selama sleep)
      4. write()         → kirim respons
      5. close()         → tutup koneksi

    Selama satu coroutine menunggu I/O (langkah 3), event loop BEBAS
    melayani coroutine lain. Inilah keunggulan utama arsitektur async.
    """
    async def handle_client(reader, writer):
        try:
            data = await reader.readline()
            if data:
                payload = json.loads(data.decode().strip())
                # ---- Simulasi I/O NON-BLOCKING (asyncio.sleep) ----
                # Selama sleep ini, event loop bisa melayani request lain.
                await asyncio.sleep(random.uniform(IO_DELAY_MIN, IO_DELAY_MAX))
                response = json.dumps({
                    "status": "ACK",
                    "metode": "async",
                    "lokasi": payload.get("lokasi", "?"),
                }) + "\n"
                writer.write(response.encode())
                await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def serve():
        server = await asyncio.start_server(handle_client, '127.0.0.1', port)
        async with server:
            await server.serve_forever()

    asyncio.run(serve())


# =====================================================================
# CLIENT PENGIRIM (identik untuk kedua skenario)
# =====================================================================

def _generate_data(i: int) -> dict:
    """Buat satu data dummy laporan warga (format sama dengan client_simulator.py)."""
    deskripsi = [
        "Banjir setinggi 30cm", "Kemacetan parah",
        "Pohon tumbang", "Lampu jalan mati",
        "Sampah menumpuk", "Genangan air",
    ]
    return {
        "tipe": "laporan_warga",
        "id_user": f"U-Bench-{i:05d}",
        "lokasi": random.choice(["Jaksel", "Jakpus", "Jakut", "Jakbar", "Jaktim"]),
        "deskripsi": random.choice(deskripsi),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "vector_clock": {"Client": i},
    }


async def _kirim_satu(sem: asyncio.Semaphore, host: str, port: int, data: dict) -> bool:
    """
    Kirim satu data ke server via TCP, dengan semaphore untuk
    membatasi jumlah koneksi simultan (simulasi connection pool).
    """
    async with sem:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=300
            )
            writer.write((json.dumps(data) + "\n").encode())
            await writer.drain()
            await asyncio.wait_for(reader.readline(), timeout=300)
            writer.close()
            await writer.wait_closed()
            return True
        except Exception:
            return False


async def _run_benchmark_client(port: int, jumlah: int):
    """
    Kirim `jumlah` data secara konkuren ke server (dengan batasan
    MAX_CONCURRENT koneksi simultan), ukur total waktu dari perspektif
    client.
    """
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    data_list = [_generate_data(i) for i in range(jumlah)]

    start = time.perf_counter()
    tasks = [_kirim_satu(sem, '127.0.0.1', port, d) for d in data_list]
    results = await asyncio.gather(*tasks)
    end = time.perf_counter()

    sukses = sum(1 for r in results if r)
    return end - start, sukses


# =====================================================================
# UTILITAS
# =====================================================================

def _wait_for_port(port: int, timeout: int = 15) -> bool:
    """Tunggu sampai port TCP menerima koneksi."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            s = socket.socket()
            s.settimeout(1)
            s.connect(('127.0.0.1', port))
            s.close()
            return True
        except Exception:
            time.sleep(0.2)
    return False


def _cetak_hasil(jumlah, t_seq, sukses_seq, t_async, sukses_async):
    """Cetak tabel perbandingan hasil benchmark."""
    throughput_seq = sukses_seq / t_seq if t_seq > 0 else 0
    throughput_async = sukses_async / t_async if t_async > 0 else 0
    speedup = t_seq / t_async if t_async > 0 else 0

    print()
    print("╔" + "═" * 62 + "╗")
    print("║" + "BENCHMARK E2E — Sequential vs Asynchronous".center(62) + "║")
    print("║" + "Studi Kasus: Jakarta Smart City (JSC)".center(62) + "║")
    print("╠" + "═" * 62 + "╣")
    print("║" + f"  Jumlah request       : {jumlah:,} data".ljust(62) + "║")
    print("║" + f"  Koneksi simultan maks: {MAX_CONCURRENT}".ljust(62) + "║")
    print("║" + f"  Simulasi delay I/O   : {IO_DELAY_MIN}s — {IO_DELAY_MAX}s per data".ljust(62) + "║")
    print("╠" + "═" * 20 + "╦" + "═" * 20 + "╦" + "═" * 20 + "╣")
    print("║" + " Metrik".ljust(20) + "║" + " Sequential".ljust(20) + "║" + " Asynchronous".ljust(20) + "║")
    print("╠" + "═" * 20 + "╬" + "═" * 20 + "╬" + "═" * 20 + "╣")
    print("║" + " Waktu Total".ljust(20) + "║" + f" {t_seq:.4f} detik".ljust(20) + "║" + f" {t_async:.4f} detik".ljust(20) + "║")
    print("║" + " Throughput".ljust(20) + "║" + f" {throughput_seq:.2f} req/s".ljust(20) + "║" + f" {throughput_async:.2f} req/s".ljust(20) + "║")
    print("║" + " Req. Sukses".ljust(20) + "║" + f" {sukses_seq}/{jumlah}".ljust(20) + "║" + f" {sukses_async}/{jumlah}".ljust(20) + "║")
    print("║" + " Speedup".ljust(20) + "║" + " 1.00x".ljust(20) + "║" + f" {speedup:.2f}x".ljust(20) + "║")
    print("╚" + "═" * 20 + "╩" + "═" * 20 + "╩" + "═" * 20 + "╝")
    print()
    print(f"  Rumus  : Speedup = T_sequential / T_async")
    print(f"         = {t_seq:.4f} / {t_async:.4f}")
    print(f"         = {speedup:.2f}x")
    print()
    print(f"  Analisis:")
    print(f"    - Server sequential memproses request satu-per-satu secara blocking.")
    print(f"      Throughput terbatas oleh delay I/O setiap request.")
    print(f"    - Server asynchronous memproses hingga {MAX_CONCURRENT} request bersamaan.")
    print(f"      Saat satu request menunggu I/O, event loop melayani request lain.")
    print(f"    - Speedup {speedup:.1f}x membuktikan keunggulan arsitektur async")
    print(f"      untuk workload I/O-bound pada sistem terdistribusi.")
    print()


# =====================================================================
# MAIN — ORCHESTRATOR
# =====================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark Kinerja E2E Sequential vs Async — Jakarta Smart City"
    )
    parser.add_argument("--jumlah", type=int, default=100,
                        help="Jumlah request untuk diuji (default: 100)")
    # Argumen internal (untuk menjalankan server sebagai subprocess)
    parser.add_argument("--mode", choices=["benchmark", "server-seq", "server-async"],
                        default="benchmark", help=argparse.SUPPRESS)
    parser.add_argument("--port", type=int, default=18500, help=argparse.SUPPRESS)
    args = parser.parse_args()

    # ---- Mode subprocess: jalankan server ----
    if args.mode == "server-seq":
        _run_sequential_server(args.port)
        return
    elif args.mode == "server-async":
        _run_async_server(args.port)
        return

    # ---- Mode utama: jalankan benchmark ----
    jumlah = args.jumlah
    port_seq = 18500
    port_async = 18501

    print(f"\n{'='*64}")
    print(f"  BENCHMARK KINERJA E2E — Poin 6 Analisis Kinerja")
    print(f"  Sequential Server vs Asynchronous Server")
    print(f"  Jumlah request : {jumlah:,}")
    print(f"  Max koneksi    : {MAX_CONCURRENT} simultan")
    print(f"{'='*64}")

    # --------------------------------------------------------
    # TEST 1: Server SEQUENTIAL
    # --------------------------------------------------------
    print(f"\n{'─'*64}")
    print(f"  SKENARIO 1: Server SEQUENTIAL (Blocking)")
    print(f"  → socket.accept() + time.sleep() → satu request per waktu")
    print(f"{'─'*64}")

    print(f"\n  [1] Menjalankan server sequential di port {port_seq}...", end="", flush=True)
    proc_seq = subprocess.Popen(
        [PYTHON, SCRIPT, "--mode", "server-seq", "--port", str(port_seq)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    if not _wait_for_port(port_seq):
        print(" GAGAL")
        proc_seq.terminate()
        return
    print(" OK")

    print(f"  [2] Mengirim {jumlah:,} request secara konkuren...")
    t_seq, sukses_seq = asyncio.run(_run_benchmark_client(port_seq, jumlah))
    print(f"      Selesai dalam {t_seq:.4f} detik ({sukses_seq}/{jumlah} sukses)")

    proc_seq.terminate()
    proc_seq.wait()
    time.sleep(1)  # Beri waktu port dilepas oleh OS

    # --------------------------------------------------------
    # TEST 2: Server ASYNCHRONOUS
    # --------------------------------------------------------
    print(f"\n{'─'*64}")
    print(f"  SKENARIO 2: Server ASYNCHRONOUS (Non-blocking)")
    print(f"  → asyncio.start_server() + asyncio.sleep() → konkuren")
    print(f"{'─'*64}")

    print(f"\n  [1] Menjalankan server async di port {port_async}...", end="", flush=True)
    proc_async = subprocess.Popen(
        [PYTHON, SCRIPT, "--mode", "server-async", "--port", str(port_async)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    if not _wait_for_port(port_async):
        print(" GAGAL")
        proc_async.terminate()
        return
    print(" OK")

    print(f"  [2] Mengirim {jumlah:,} request secara konkuren...")
    t_async, sukses_async = asyncio.run(_run_benchmark_client(port_async, jumlah))
    print(f"      Selesai dalam {t_async:.4f} detik ({sukses_async}/{jumlah} sukses)")

    proc_async.terminate()
    proc_async.wait()

    # --------------------------------------------------------
    # HASIL
    # --------------------------------------------------------
    _cetak_hasil(jumlah, t_seq, sukses_seq, t_async, sukses_async)


if __name__ == "__main__":
    main()
