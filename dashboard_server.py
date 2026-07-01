"""
dashboard_server.py
====================
HTTP server untuk Dashboard Monitoring Jakarta Smart City.

Menyajikan halaman web dashboard.html dan endpoint API yang
membaca data status dari main_server.py.

Cara menjalankan:
    python dashboard_server.py --port 8080
"""

import http.server
import json
import os
import argparse
from urllib.parse import urlparse

# Base directory = same folder as this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

STATUS_FILE = os.path.join(BASE_DIR, "status.json")
DB_LAPORAN = os.path.join(BASE_DIR, "db_laporan_warga.json")
DB_SENSOR = os.path.join(BASE_DIR, "db_log_sensor.json")
DASHBOARD_HTML = os.path.join(BASE_DIR, "dashboard.html")


class DashboardHandler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):
        path = urlparse(self.path).path

        if path == '/' or path == '/dashboard.html':
            self._serve_file(DASHBOARD_HTML, 'text/html')
        elif path == '/api/status':
            self._serve_json_file(STATUS_FILE)
        elif path == '/api/db/recent':
            self._serve_recent_db()
        else:
            self.send_error(404)

    def _serve_file(self, filepath, content_type):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read().encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', f'{content_type}; charset=utf-8')
            self.send_header('Content-Length', len(content))
            self.send_header('Cache-Control', 'no-cache')
            self.end_headers()
            self.wfile.write(content)
        except FileNotFoundError:
            self.send_error(404, f'File not found: {filepath}')

    def _serve_json_file(self, filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read().encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Content-Length', len(content))
            self.end_headers()
            self.wfile.write(content)
        except FileNotFoundError:
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(b'{}')

    def _serve_recent_db(self):
        recent = []
        for db_path in [DB_LAPORAN, DB_SENSOR]:
            if os.path.exists(db_path):
                try:
                    with open(db_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    if isinstance(data, list):
                        recent.extend(data)
                except (json.JSONDecodeError, IOError):
                    pass

        # Sort by timestamp descending, take last 20
        recent.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        recent = recent[:20]

        content = json.dumps({'recent': recent}, ensure_ascii=False).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Content-Length', len(content))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format, *args):
        # Suppress default request logging to keep terminal clean
        pass


def main():
    parser = argparse.ArgumentParser(description='Dashboard Server - Jakarta Smart City')
    parser.add_argument('--port', type=int, default=8080)
    args = parser.parse_args()

    server = http.server.HTTPServer(('0.0.0.0', args.port), DashboardHandler)
    print(f'[Dashboard] Server aktif di http://localhost:{args.port}')
    print(f'[Dashboard] Buka browser dan akses http://localhost:{args.port}\n')

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\n[Dashboard] Server dihentikan.')
        server.server_close()


if __name__ == '__main__':
    main()
