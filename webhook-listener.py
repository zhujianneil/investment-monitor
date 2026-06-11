from http.server import HTTPServer, BaseHTTPRequestHandler
import subprocess
import hmac
import hashlib
import os

SECRET = os.environ.get("WEBHOOK_SECRET", "change-me")

class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/webhook":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        sig = self.headers.get("X-Hub-Signature-256", "")
        expected = "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b"Forbidden")
            return

        event = self.headers.get("X-GitHub-Event", "")
        if event != "push":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Ignored")
            return

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK - deploying...")

        subprocess.Popen([
            "/bin/bash", "-c",
            "cd /opt/investment-monitor && "
            "git pull && "
            "docker-compose build && "
            "docker-compose up -d && "
            "echo 'deploy done at $(date)' >> /opt/investment-monitor/logs/webhook.log"
        ])

    def log_message(self, format, *args):
        print(f"[webhook] {args[0]}")

if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 9000), WebhookHandler)
    print("Webhook listener on :9000")
    server.serve_forever()
