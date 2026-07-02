"""
client_simulator.py
====================
Simulasi lapisan Client/User - Jakarta Smart City (JSC)

Mensimulasikan aplikasi warga (JAKI) yang mengirim laporan, dan sensor
CCTV yang mengirim status kepadatan, ke API Gateway melalui HTTP/REST.

Catatan: skrip ini BUKAN salah satu dari dua implementasi wajib poin 5,
melainkan pembangkit beban (traffic generator) untuk membuktikan
api_gateway.py -> edge_node.py -> main_server.py bekerja end-to-end.

Fitur tambahan:
  - Poin 6 (Konsistensi Data - Vector Clock):
        Setiap data yang dikirim di-tag dengan vector clock sederhana
        (counter per-client) agar urutan pengiriman dapat dilacak.

  - Poin 7 (Fault Tolerance - Chaos Mode):
        Dengan flag --chaos, simulator akan secara acak (10% probabilitas)
        menutup koneksi segera setelah mengirim data TANPA menunggu
        respons dari Edge Node. Ini mensimulasikan crash client atau
        putus koneksi secara tiba-tiba, berguna untuk menguji ketahanan
        Edge Node terhadap kegagalan tak terduga.

Cara menjalankan:
    python client_simulator.py --port 8000 --region Jaksel --jumlah 50
    python client_simulator.py --port 8000 --region Jaksel --jumlah 100 --chaos
"""

import asyncio
import json
import random
import argparse
from datetime import datetime

DESKRIPSI_LAPORAN = [
    "Banjir setinggi 30cm di area jalan utama",
    "Kemacetan parah akibat kendaraan mogok",
    "Pohon tumbang menutup sebagian jalan",
    "Lampu jalan mati di beberapa titik",
    "Sampah menumpuk di pinggir saluran air",
    "Genangan air pasca hujan deras",
]

STATUS_SENSOR = ["lancar", "padat", "macet total", "siaga banjir"]

client_vc = {"Client": 0}

def _increment_vc() -> dict:
    """Increment vector clock client dan kembalikan salinannya."""
    client_vc["Client"] += 1
    return dict(client_vc)

def buat_laporan_warga(i: int, region: str) -> dict:
    """Buat satu data dummy laporan warga."""
    return {
        "tipe": "laporan_warga",
        "id_user": f"U-{region}-{i:04d}",
        "lokasi": region,
        "foto": f"foto_{i:04d}.jpg",
        "deskripsi": random.choice(DESKRIPSI_LAPORAN),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "vector_clock": _increment_vc(),
    }

def buat_log_sensor(i: int, region: str) -> dict:
    """Buat satu data dummy log sensor CCTV."""
    return {
        "tipe": "log_sensor",
        "id_cctv": f"CCTV-{region}-{i:04d}",
        "lokasi": region,
        "status_kepadatan": random.choice(STATUS_SENSOR),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "vector_clock": _increment_vc(),
    }

async def kirim_satu_via_gateway(host: str, port: int, data: dict, chaos: bool = False) -> str:
    """
    Mengirim satu data ke API Gateway via HTTP/1.1 POST, lalu gateway
    meneruskan ke Edge Node yang sesuai.
    """
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    request = (
        f"POST /api/ingest HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n\r\n"
    ).encode("utf-8") + body

    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=10
        )
        writer.write(request)
        await writer.drain()

        if chaos and random.random() < 0.1:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return "chaos_drop"

        response = await asyncio.wait_for(reader.read(), timeout=10)
        writer.close()
        await writer.wait_closed()

        if b"200 OK" in response:
            return "sukses"
        return "gagal"

    except (ConnectionRefusedError, OSError, asyncio.TimeoutError, Exception):
        return "gagal"


async def main(host: str, port: int, region: str, jumlah: int, chaos: bool):
    """
    Mengirim sejumlah data dummy (campuran laporan warga & sensor CCTV)
    secara bersamaan (concurrent) ke Edge Node.

    Jika chaos mode aktif, sebagian koneksi akan sengaja diputus untuk
    menguji fault tolerance Edge Node.
    """
    mode_str = " [CHAOS MODE AKTIF - 10% koneksi akan diputus paksa]" if chaos else ""
    target_str = f"API Gateway di {host}:{port}"
    print(
        f"[Client] mengirim {jumlah} data dummy (campuran laporan warga & sensor CCTV) "
        f"ke {target_str}{mode_str} ...\n"
    )

    tugas = []
    for i in range(jumlah):
        data = buat_laporan_warga(i, region) if random.random() < 0.6 else buat_log_sensor(i, region)
        tugas.append(kirim_satu_via_gateway(host, port, data, chaos=chaos))

    hasil = await asyncio.gather(*tugas)

    sukses = sum(1 for r in hasil if r == "sukses")
    gagal = sum(1 for r in hasil if r == "gagal")
    dropped = sum(1 for r in hasil if r == "chaos_drop")

    print(f"\n[Client] === HASIL PENGIRIMAN ===")
    print(f"  Total data      : {jumlah}")
    print(f"  Sukses          : {sukses}")
    print(f"  Gagal           : {gagal}", end="")
    if gagal > 0:
        print(" (edge_node.py belum dijalankan / unreachable)", end="")
    print()
    if chaos:
        print(f"  Chaos drop      : {dropped} (koneksi sengaja diputus)")
    print(f"  Vector Clock    : {client_vc}")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulasi client warga & sensor CCTV")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True, help="Port API Gateway tujuan")
    parser.add_argument("--region", required=True, help="Nama wilayah tujuan")
    parser.add_argument("--jumlah", type=int, default=50, help="Jumlah data dummy yang dikirim")
    parser.add_argument("--chaos", action="store_true", default=False,
                        help="Aktifkan chaos mode: 10%% koneksi diputus paksa untuk uji fault tolerance")
    args = parser.parse_args()

    asyncio.run(main(args.host, args.port, args.region, args.jumlah, args.chaos))
