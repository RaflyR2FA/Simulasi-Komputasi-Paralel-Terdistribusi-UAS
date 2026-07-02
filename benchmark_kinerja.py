"""
benchmark_kinerja.py
====================
Poin 6 - Analisis Kinerja: Sequential vs Asynchronous
Studi Kasus: Jakarta Smart City (JSC)

Script ini membandingkan kinerja pemrosesan data laporan warga
secara sequential (blocking) vs asynchronous (non-blocking).

Metrik yang diukur:
  - Execution Time (detik)
  - Throughput (data/detik) 
  - Speedup = T_sequential / T_async

Cara menjalankan:
    python benchmark_kinerja.py --jumlah 100
    python benchmark_kinerja.py --jumlah 1000
    python benchmark_kinerja.py --jumlah 10000
"""

import asyncio
import time
import random
import argparse
import sys
import io
from datetime import datetime

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

IO_DELAY_MIN = 0.01  
IO_DELAY_MAX = 0.05


def generate_dummy_data(n: int) -> list:
    """Buat N data dummy laporan warga."""
    deskripsi = [
        "Banjir setinggi 30cm",
        "Kemacetan parah", 
        "Pohon tumbang",
        "Lampu jalan mati",
        "Sampah menumpuk",
        "Genangan air",
    ]
    data = []
    for i in range(n):
        data.append({
            "tipe": "laporan_warga",
            "id_user": f"U-Bench-{i:05d}",
            "lokasi": random.choice(["Jaksel", "Jakpus", "Jakut", "Jakbar", "Jaktim"]),
            "deskripsi": random.choice(deskripsi),
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        })
    return data


def proses_sequential(data_list: list) -> float:
    """
    Pemrosesan SEQUENTIAL (blocking).
    Setiap laporan diproses satu per satu — laporan ke-2 harus
    menunggu laporan ke-1 selesai.
    """
    start = time.perf_counter()
    for data in data_list:
        delay = random.uniform(IO_DELAY_MIN, IO_DELAY_MAX)
        time.sleep(delay)
        data["status"] = "diproses"
        data["metode"] = "sequential"
    end = time.perf_counter()
    return end - start


async def _proses_satu_async(data: dict):
    """Proses satu data secara async (non-blocking)."""
    delay = random.uniform(IO_DELAY_MIN, IO_DELAY_MAX)
    await asyncio.sleep(delay)
    data["status"] = "diproses"
    data["metode"] = "async"


def proses_async(data_list: list) -> float:
    """
    Pemrosesan ASYNCHRONOUS (non-blocking).
    Semua laporan diproses secara konkuren — event loop berpindah
    ke laporan lain saat satu sedang menunggu I/O.
    """
    async def _run():
        tasks = [_proses_satu_async(d) for d in data_list]
        await asyncio.gather(*tasks)
    
    start = time.perf_counter()
    asyncio.run(_run())
    end = time.perf_counter()
    return end - start


def cetak_hasil(jumlah: int, t_seq: float, t_async: float):
    """Cetak tabel perbandingan hasil benchmark."""
    throughput_seq = jumlah / t_seq if t_seq > 0 else 0
    throughput_async = jumlah / t_async if t_async > 0 else 0
    speedup = t_seq / t_async if t_async > 0 else 0
    
    print()
    print("╔" + "═" * 62 + "╗")
    print("║" + "ANALISIS KINERJA — Sequential vs Asynchronous".center(62) + "║")
    print("║" + "Studi Kasus: Jakarta Smart City (JSC)".center(62) + "║")
    print("╠" + "═" * 62 + "╣")
    print("║" + f"  Jumlah data uji    : {jumlah:,} laporan warga".ljust(62) + "║")
    print("║" + f"  Simulasi delay I/O : {IO_DELAY_MIN}s — {IO_DELAY_MAX}s per data".ljust(62) + "║")
    print("╠" + "═" * 20 + "╦" + "═" * 20 + "╦" + "═" * 20 + "╣")
    print("║" + " Metrik".ljust(20) + "║" + " Sequential".ljust(20) + "║" + " Asynchronous".ljust(20) + "║")
    print("╠" + "═" * 20 + "╬" + "═" * 20 + "╬" + "═" * 20 + "╣")
    print("║" + " Exec. Time".ljust(20) + "║" + f" {t_seq:.4f} detik".ljust(20) + "║" + f" {t_async:.4f} detik".ljust(20) + "║")
    print("║" + " Throughput".ljust(20) + "║" + f" {throughput_seq:.2f} data/s".ljust(20) + "║" + f" {throughput_async:.2f} data/s".ljust(20) + "║")
    print("║" + " Speedup".ljust(20) + "║" + f" 1.00x".ljust(20) + "║" + f" {speedup:.2f}x".ljust(20) + "║")
    print("╚" + "═" * 20 + "╩" + "═" * 20 + "╩" + "═" * 20 + "╝")
    print()
    print(f"  Rumus Speedup: S = T_sequential / T_async = {t_seq:.4f} / {t_async:.4f} = {speedup:.2f}x")
    print(f"  Kesimpulan: Pendekatan asinkron {speedup:.1f}x lebih cepat dari sequential.")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark Kinerja Sequential vs Async — Jakarta Smart City"
    )
    parser.add_argument("--jumlah", type=int, default=100,
                       help="Jumlah data laporan warga untuk diuji (default: 100)")
    args = parser.parse_args()
    
    jumlah = args.jumlah
    print(f"\n{'='*64}")
    print(f"  BENCHMARK KINERJA — Poin 6 Analisis Kinerja")
    print(f"  Sequential vs Asynchronous Processing")
    print(f"  Jumlah data: {jumlah:,}")
    print(f"{'='*64}")
    
    print(f"\n[1/3] Membuat {jumlah:,} data dummy...")
    data_seq = generate_dummy_data(jumlah)
    data_async = generate_dummy_data(jumlah)
    
    print(f"[2/3] Menjalankan pemrosesan SEQUENTIAL (blocking)...")
    t_seq = proses_sequential(data_seq)
    print(f"      Selesai dalam {t_seq:.4f} detik")
    
    print(f"[3/3] Menjalankan pemrosesan ASYNCHRONOUS (non-blocking)...")
    t_async = proses_async(data_async)
    print(f"      Selesai dalam {t_async:.4f} detik")
    
    cetak_hasil(jumlah, t_seq, t_async)


if __name__ == "__main__":
    main()
