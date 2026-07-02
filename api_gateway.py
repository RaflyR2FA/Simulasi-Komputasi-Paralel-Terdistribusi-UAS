"""
api_gateway.py
==============
API Gateway untuk Jakarta Smart City (JSC).

Komponen ini melengkapi arsitektur pada jsc_distributed_architecture.png:

  - Menerima request HTTP/REST dari aplikasi warga (JAKI) dan perangkat
    CCTV/IoT.
  - Melakukan routing berdasarkan wilayah ke Edge Node yang sesuai.
  - Menyediakan load balancing sederhana per wilayah dengan round-robin.
  - Meneruskan payload ke Edge Node via TCP socket, sehingga Edge Node
    tetap menjadi lapisan preprocessing/validasi seperti pada diagram.

Cara menjalankan:
    python api_gateway.py --port 8000
"""

from __future__ import annotations

import argparse
import json
import socket
import threading
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Tuple


DEFAULT_ROUTES: Dict[str, List[Tuple[str, int]]] = {
    "Jakpus": [("127.0.0.1", 9001)],
    "Jakut": [("127.0.0.1", 9002)],
    "Jakbar": [("127.0.0.1", 9003)],
    "Jaksel": [("127.0.0.1", 9004)],
    "Jaktim": [("127.0.0.1", 9005)],
}


class GatewayState:
    def __init__(self):
        self.routes = {region: list(endpoints) for region, endpoints in DEFAULT_ROUTES.items()}
        self.route_lock = threading.Lock()
        self.round_robin_index = defaultdict(int)

    def set_routes(self, routes: Dict[str, List[Tuple[str, int]]]):
        with self.route_lock:
            self.routes = {region: list(endpoints) for region, endpoints in routes.items()}
            self.round_robin_index = defaultdict(int)

    def pick_endpoint(self, region: str) -> Tuple[str, int]:
        with self.route_lock:
            candidates = self.routes.get(region, [])
            if not candidates:
                raise KeyError(region)

            index = self.round_robin_index[region] % len(candidates)
            self.round_robin_index[region] += 1
            return candidates[index]


gateway_state = GatewayState()


def _forward_to_edge(host: str, port: int, payload: dict) -> dict:
    message = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
    with socket.create_connection((host, port), timeout=10) as sock:
        sock.settimeout(10)
        sock.sendall(message)

        response = b""
        while not response.endswith(b"\n"):
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk

    if not response:
        raise TimeoutError("Edge node tidak memberikan respons")

    return json.loads(response.decode("utf-8").strip())


class GatewayHandler(BaseHTTPRequestHandler):
    server_version = "JSCGateway/1.0"

    def do_GET(self):
        if self.path == "/health":
            self._send_json(200, {"status": "ok", "service": "api_gateway"})
            return

        if self.path == "/routes":
            with gateway_state.route_lock:
                routes = {
                    region: [{"host": host, "port": port} for host, port in endpoints]
                    for region, endpoints in gateway_state.routes.items()
                }
            self._send_json(200, {"routes": routes})
            return

        self.send_error(404)

    def do_POST(self):
        if self.path != "/api/ingest":
            self.send_error(404)
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length)

        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError:
            self._send_json(400, {"status": "error", "pesan": "body JSON tidak valid"})
            return

        region = payload.get("lokasi") or payload.get("region")
        if not region:
            self._send_json(400, {"status": "error", "pesan": "field lokasi/region wajib ada"})
            return

        try:
            host, port = gateway_state.pick_endpoint(region)
        except KeyError:
            self._send_json(404, {"status": "error", "pesan": f"region {region} tidak terdaftar"})
            return

        try:
            hasil = _forward_to_edge(host, port, payload)
        except Exception as exc:
            self._send_json(
                502,
                {
                    "status": "error",
                    "pesan": f"gagal meneruskan ke edge node {region}",
                    "detail": str(exc),
                },
            )
            return

        self._send_json(200, {
            "status": "diterima",
            "gateway": "api_gateway",
            "region": region,
            "edge_node": {"host": host, "port": port},
            "response": hasil,
        })

    def _send_json(self, status_code: int, payload: dict):
        content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format, *args):
        pass


def parse_routes(route_args: List[str]) -> Dict[str, List[Tuple[str, int]]]:
    routes = {region: list(endpoints) for region, endpoints in DEFAULT_ROUTES.items()}

    for item in route_args:
        try:
            region, target = item.split("=", 1)
            host, port_text = target.rsplit(":", 1)
            port = int(port_text)
        except ValueError as exc:
            raise ValueError(f"Format route tidak valid: {item}. Gunakan Region=host:port") from exc

        routes.setdefault(region, []).append((host, port))

    return routes


def main():
    parser = argparse.ArgumentParser(description="API Gateway Jakarta Smart City")
    parser.add_argument("--host", default="0.0.0.0", help="Host bind untuk gateway")
    parser.add_argument("--port", type=int, default=8000, help="Port HTTP gateway")
    parser.add_argument(
        "--route",
        action="append",
        default=[],
        help="Override/menambah route per wilayah. Format: Region=host:port",
    )
    args = parser.parse_args()

    gateway_state.set_routes(parse_routes(args.route))

    server = ThreadingHTTPServer((args.host, args.port), GatewayHandler)
    print("=" * 68)
    print("  Jakarta Smart City — API Gateway")
    print("=" * 68)
    print(f"  HTTP endpoint  : http://{args.host}:{args.port}")
    print("  Endpoint utama : POST /api/ingest")
    print("  Health check   : GET  /health")
    print("  Routes         : GET  /routes")
    print("=" * 68)
    print()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Gateway] Server dihentikan.")
        server.server_close()


if __name__ == "__main__":
    main()