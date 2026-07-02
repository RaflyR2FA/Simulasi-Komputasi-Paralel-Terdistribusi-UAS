"""
simulasi_fault.py
=================
Simulasi Skenario Fault Tolerance — Jakarta Smart City (JSC)

Script ini secara otomatis menjalankan tiga skenario kegagalan
untuk membuktikan mekanisme fault tolerance pada sistem:

  Skenario 1: Client crash di tengah pengiriman
  Skenario 2: Main Server down → Edge Node retry → recovery
  Skenario 3: Edge Node offline → Main Server deteksi

Cara menjalankan:
    python simulasi_fault.py

Prerequisite: file edge_node.py, main_server.py, dan
client_simulator.py harus ada di folder yang sama.
"""

import asyncio
import json
import subprocess
import sys
import time
import os
import signal
import socket
import io

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON = sys.executable

MAIN_PORT = 19000
EDGE_PORT = 19001


def print_header(text):
    print(f"\n{'='*64}")
    print(f"  {text}")
    print(f"{'='*64}")


def print_step(num, text, status=""):
    status_str = f"  {status}" if status else ""
    print(f"  [STEP {num}] {text}{status_str}")


def print_result(text):
    print(f"  [HASIL] {text}")


def wait_for_port(port, timeout=10):
    """Wait until a port is accepting connections."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect(('127.0.0.1', port))
            s.close()
            return True
        except (ConnectionRefusedError, socket.timeout, OSError):
            time.sleep(0.3)
    return False


def kill_proc(proc):
    """Safely kill a subprocess."""
    if proc and proc.poll() is None:
        try:
            if sys.platform == 'win32':
                proc.terminate()
            else:
                os.kill(proc.pid, signal.SIGTERM)
            proc.wait(timeout=5)
        except Exception:
            proc.kill()


async def send_data(port, data_dict):
    """Send one JSON line to a TCP port and get response."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection('127.0.0.1', port), timeout=5
        )
        writer.write((json.dumps(data_dict) + '\n').encode())
        await writer.drain()
        response = await asyncio.wait_for(reader.readline(), timeout=5)
        writer.close()
        await writer.wait_closed()
        return json.loads(response.decode().strip())
    except Exception as e:
        return None


async def send_data_and_crash(port, data_dict):
    """Send data then immediately close connection (simulate crash)."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection('127.0.0.1', port), timeout=5
        )
        writer.write((json.dumps(data_dict) + '\n').encode())
        await writer.drain()
        writer.close()
        return True
    except Exception:
        return False


def make_laporan(i):
    return {
        "tipe": "laporan_warga",
        "id_user": f"U-FAULT-{i:04d}",
        "lokasi": "Jaksel",
        "deskripsi": "Test fault tolerance",
        "timestamp": "2026-01-01T00:00:00",
        "vector_clock": {"Client": i}
    }


def run_skenario_1():
    """Skenario 1: Client crash di tengah pengiriman."""
    print_header("Skenario 1: Client Crash di Tengah Pengiriman")
    print("  Membuktikan: satu klien crash TIDAK menjatuhkan Edge Node,")
    print("  klien lain tetap terlayani (Referensi [8])\n")

    print_step(1, "Menjalankan Edge Node Jaksel...", end="")
    edge = subprocess.Popen(
        [PYTHON, os.path.join(BASE_DIR, 'edge_node.py'),
         '--region', 'Jaksel', '--port', str(EDGE_PORT),
         '--main-port', str(MAIN_PORT), '--sync-interval', '999'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if wait_for_port(EDGE_PORT):
        print("✓")
    else:
        print("✗ (gagal start)")
        kill_proc(edge)
        return False

    try:
        print_step(2, "Mengirim 5 data normal...", end="")
        results = asyncio.run(_send_batch(EDGE_PORT, 5))
        sukses = sum(1 for r in results if r)
        print(f"✓ ({sukses}/5 sukses)")

        print_step(3, "Mengirim data lalu crash (putus koneksi paksa)...", end="")
        crash_result = asyncio.run(send_data_and_crash(EDGE_PORT, make_laporan(99)))
        print("✓ (koneksi diputus)" if crash_result else "✗")

        time.sleep(0.5)
        print_step(4, "Mengirim 5 data lagi dari client baru...", end="")
        results2 = asyncio.run(_send_batch(EDGE_PORT, 5, start=10))
        sukses2 = sum(1 for r in results2 if r)
        print(f"✓ ({sukses2}/5 sukses)")

        if sukses2 == 5:
            print_result("Edge Node TIDAK crash, client lain tetap terlayani ✓")
            return True
        else:
            print_result("Edge Node terganggu ✗")
            return False
    finally:
        kill_proc(edge)


async def _send_batch(port, count, start=0):
    tasks = [send_data(port, make_laporan(start + i)) for i in range(count)]
    return await asyncio.gather(*tasks)


def run_skenario_2():
    """Skenario 2: Main Server down → retry → recovery."""
    print_header("Skenario 2: Main Server Down → Edge Node Retry → Recovery")
    print("  Membuktikan: data tidak hilang saat server pusat down,")
    print("  terkirim otomatis saat server pulih\n")

    print_step(1, "Menjalankan Edge Node TANPA Main Server...", end="")
    edge = subprocess.Popen(
        [PYTHON, os.path.join(BASE_DIR, 'edge_node.py'),
         '--region', 'Jaksel', '--port', str(EDGE_PORT),
         '--main-port', str(MAIN_PORT), '--sync-interval', '3'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if wait_for_port(EDGE_PORT):
        print("✓")
    else:
        print("✗")
        kill_proc(edge)
        return False

    main_proc = None
    try:
        print_step(2, "Mengirim 5 data ke Edge Node (Main Server belum aktif)...", end="")
        results = asyncio.run(_send_batch(EDGE_PORT, 5))
        sukses = sum(1 for r in results if r)
        print(f"✓ ({sukses}/5 diterima Edge Node, masuk buffer)")

        print_step(3, "Menunggu Edge Node mencoba sync (akan gagal)...")
        time.sleep(5)
        print("          ↳ Sync seharusnya gagal, data tetap di buffer")

        print_step(4, "Menjalankan Main Server...", end="")
        for f in ['db_laporan_warga.json', 'db_log_sensor.json', 'status.json']:
            p = os.path.join(BASE_DIR, f)
            if os.path.exists(p):
                os.remove(p)
        
        main_proc = subprocess.Popen(
            [PYTHON, os.path.join(BASE_DIR, 'main_server.py'),
             '--port', str(MAIN_PORT)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if wait_for_port(MAIN_PORT):
            print("✓")
        else:
            print("✗")
            return False

        print_step(5, "Menunggu Edge Node sync ulang (seharusnya berhasil)...")
        time.sleep(6)

        db_path = os.path.join(BASE_DIR, 'db_laporan_warga.json')
        if os.path.exists(db_path):
            with open(db_path, 'r') as f:
                db = json.load(f)
            print(f"          ↳ Data di DB Main Server: {len(db)} laporan")
            if len(db) >= sukses:
                print_result(f"Data tidak hilang! {len(db)} laporan berhasil tersinkronisasi setelah recovery ✓")
                return True
            else:
                print_result(f"Data hilang: hanya {len(db)} dari {sukses} ✗")
                return False
        else:
            db_sensor_path = os.path.join(BASE_DIR, 'db_log_sensor.json') 
            total = 0
            for p in [db_path, db_sensor_path]:
                if os.path.exists(p):
                    with open(p, 'r') as f:
                        total += len(json.load(f))
            if total >= sukses:
                print_result(f"Data tidak hilang! {total} data berhasil tersinkronisasi ✓")
                return True
            print_result("DB file belum terbentuk — sync mungkin belum terjadi ✗")
            return False
    finally:
        kill_proc(edge)
        kill_proc(main_proc)
        for f in ['db_laporan_warga.json', 'db_log_sensor.json', 'status.json']:
            p = os.path.join(BASE_DIR, f)
            if os.path.exists(p):
                try:
                    os.remove(p)
                except:
                    pass


def run_skenario_3():
    """Skenario 3: Edge Node offline detection."""
    print_header("Skenario 3: Edge Node Offline → Main Server Deteksi")
    print("  Membuktikan: Main Server mendeteksi node yang down")
    print("  dan mencatat alert (heartbeat monitoring)\n")

    print_step(1, "Menjalankan Main Server...", end="")
    for f in ['db_laporan_warga.json', 'db_log_sensor.json', 'status.json']:
        p = os.path.join(BASE_DIR, f)
        if os.path.exists(p):
            os.remove(p)
    
    main_proc = subprocess.Popen(
        [PYTHON, os.path.join(BASE_DIR, 'main_server.py'),
         '--port', str(MAIN_PORT)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if wait_for_port(MAIN_PORT):
        print("✓")
    else:
        print("✗")
        kill_proc(main_proc)
        return False

    edge = None
    try:
        print_step(2, "Menjalankan Edge Node Jaksel...", end="")
        edge = subprocess.Popen(
            [PYTHON, os.path.join(BASE_DIR, 'edge_node.py'),
             '--region', 'Jaksel', '--port', str(EDGE_PORT),
             '--main-port', str(MAIN_PORT), '--sync-interval', '2'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if wait_for_port(EDGE_PORT):
            print("✓")
        else:
            print("✗")
            return False

        print_step(3, "Mengirim data untuk trigger sync awal...", end="")
        results = asyncio.run(_send_batch(EDGE_PORT, 3))
        print(f"✓ ({sum(1 for r in results if r)}/3)")

        print_step(4, "Menunggu sync pertama...")
        time.sleep(4)

        print_step(5, "Mematikan Edge Node Jaksel secara paksa...", end="")
        kill_proc(edge)
        edge = None
        print("✓ (node dimatikan)")

        print_step(6, "Menunggu Main Server mendeteksi node offline (30+ detik)...")
        time.sleep(35)

        status_path = os.path.join(BASE_DIR, 'status.json')
        if os.path.exists(status_path):
            with open(status_path, 'r') as f:
                status = json.load(f)
            
            registry = status.get('node_registry', {})
            alerts = status.get('alerts', [])
            
            jaksel_status = registry.get('Jaksel', {}).get('status', 'UNKNOWN')
            has_offline_alert = any('OFFLINE' in a.get('pesan', '') for a in alerts)
            
            print(f"          ↳ Status Jaksel di registry: {jaksel_status}")
            print(f"          ↳ Alert OFFLINE tercatat: {'Ya' if has_offline_alert else 'Tidak'}")
            
            if jaksel_status == 'OFFLINE' or has_offline_alert:
                print_result("Main Server berhasil mendeteksi Edge Node OFFLINE ✓")
                return True
            else:
                print_result("Deteksi belum tercatat di status.json ✗")
                return False
        else:
            print_result("status.json belum terbentuk ✗")
            return False
    finally:
        if edge:
            kill_proc(edge)
        kill_proc(main_proc)
        for f in ['db_laporan_warga.json', 'db_log_sensor.json', 'status.json']:
            p = os.path.join(BASE_DIR, f)
            if os.path.exists(p):
                try:
                    os.remove(p)
                except:
                    pass


def main():
    print()
    print("╔" + "═" * 62 + "╗")
    print("║" + "SIMULASI SKENARIO FAULT TOLERANCE".center(62) + "║")
    print("║" + "Jakarta Smart City (JSC)".center(62) + "║")
    print("╚" + "═" * 62 + "╝")
    
    results = {}
    
    results["Skenario 1"] = run_skenario_1()
    time.sleep(2)
    
    results["Skenario 2"] = run_skenario_2()
    time.sleep(2)
    
    results["Skenario 3"] = run_skenario_3()
    
    print_header("RINGKASAN HASIL")
    for name, passed in results.items():
        status = "✓ BERHASIL" if passed else "✗ GAGAL"
        print(f"  {name}: {status}")
    
    total_pass = sum(1 for v in results.values() if v)
    print(f"\n  Total: {total_pass}/3 skenario berhasil")
    print()


if __name__ == '__main__':
    main()
