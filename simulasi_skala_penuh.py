"""
simulasi_skala_penuh.py
=======================
Script untuk menjalankan demonstrasi Jakarta Smart City skala penuh.
Menjalankan Main Server, API Gateway, Dashboard, dan 5 Edge Node sekaligus.
Juga secara otomatis mengirimkan traffic data secara kontinu
untuk keperluan demonstrasi visual pada Dashboard.

Cara menjalankan:
    python simulasi_skala_penuh.py
"""

import subprocess
import sys
import os
import time
import signal
import io
import random

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON = sys.executable

nodes = [
    {"region": "Jakpus", "port": 9001},
    {"region": "Jakut",  "port": 9002},
    {"region": "Jakbar", "port": 9003},
    {"region": "Jaksel", "port": 9004},
    {"region": "Jaktim", "port": 9005},
]

processes = []

def cleanup(signum=None, frame=None):
    print("\n[!] Menghentikan semua layanan...")
    for p in processes:
        if p.poll() is None:
            try:
                if sys.platform == 'win32':
                    p.terminate()
                else:
                    os.kill(p.pid, signal.SIGTERM)
                p.wait(timeout=3)
            except Exception:
                p.kill()
    print("[✓] Semua layanan telah dihentikan.")
    sys.exit(0)

signal.signal(signal.SIGINT, cleanup)
if sys.platform != 'win32':
    signal.signal(signal.SIGTERM, cleanup)

def main():
    for f in ['db_laporan_warga.json', 'db_log_sensor.json', 'status.json']:
        if os.path.exists(f):
            try:
                os.remove(f)
            except OSError:
                pass

    print("=" * 64)
    print("  SIMULASI SKALA PENUH JAKARTA SMART CITY (5 NODE)")
    print("=" * 64)
    
    print("[1/5] Menjalankan Main Server (Port 9000)...")
    p_main = subprocess.Popen([PYTHON, os.path.join(BASE_DIR, 'main_server.py'), '--port', '9000'])
    processes.append(p_main)
    time.sleep(2)
    
    print("[2/5] Menjalankan API Gateway (Port 8000)...")
    p_gateway = subprocess.Popen([PYTHON, os.path.join(BASE_DIR, 'api_gateway.py'), '--port', '8000'])
    processes.append(p_gateway)
    time.sleep(1)

    print("[3/5] Menjalankan Dashboard Server (Port 8080)...")
    p_dash = subprocess.Popen([PYTHON, os.path.join(BASE_DIR, 'dashboard_server.py'), '--port', '8080'])
    processes.append(p_dash)
    time.sleep(1)
    
    print("[4/5] Menjalankan 5 Edge Node Wilayah...")
    for node in nodes:
        print(f"      - Edge Node {node['region']} (Port {node['port']})")
        p_edge = subprocess.Popen([
            PYTHON, os.path.join(BASE_DIR, 'edge_node.py'), 
            '--region', node['region'], 
            '--port', str(node['port']),
            '--main-port', '9000',
            '--sync-interval', '3'
        ])
        processes.append(p_edge)
    
    time.sleep(3)
    
    print("\n[5/5] Mulai menghasilkan traffic data kontinu melalui API Gateway...")
    print("=" * 64)
    print("  SISTEM BERJALAN SKALA PENUH!")
    print("  -> Buka browser: http://localhost:8080 untuk melihat Dashboard")
    print("  -> API Gateway aktif di http://localhost:8000")
    print("  -> Tekan Ctrl+C di terminal ini untuk menghentikan semua.")
    print("=" * 64 + "\n")
    
    try:
        while True:
            target_nodes = random.sample(nodes, k=random.randint(1, 4))
            
            for node in target_nodes:
                jumlah_data = random.randint(5, 25)
                subprocess.Popen([
                    PYTHON, os.path.join(BASE_DIR, 'client_simulator.py'),
                    '--region', node['region'],
                    '--port', '8000',
                    '--jumlah', str(jumlah_data)
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
            time.sleep(2)
            
    except KeyboardInterrupt:
        cleanup()

if __name__ == "__main__":
    main()
