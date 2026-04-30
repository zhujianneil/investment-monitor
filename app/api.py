from flask import Flask, jsonify
import sqlite3
import os

app = Flask(__name__)

# 获取当前文件所在目录的父目录作为项目根目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "investment.db")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# 从环境变量读取 API Key，如果没有则默认为空（不建议在生产环境留空）
MONITOR_API_KEY = os.getenv("MONITOR_API_KEY", "your-secret-key-change-me")

def check_auth(request):
    key = request.headers.get("X-API-KEY")
    return key == MONITOR_API_KEY

@app.route('/api/alerts')
def get_alerts():
    from flask import request
    if not check_auth(request):
        return jsonify({"error": "Unauthorized"}), 401
    
    if not os.path.exists(DB_PATH):
        return jsonify([])
    
    try:
        conn = get_db_connection()
        rows = conn.execute(
            "SELECT * FROM alerts ORDER BY triggered_at DESC LIMIT 50"
        ).fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/health')
def health():
    return jsonify({"status": "ok", "db_exists": os.path.exists(DB_PATH)})

def run_api(host='0.0.0.0', port=5001):
    print(f"Starting Monitor API on {host}:{port}...")
    app.run(host=host, port=port, debug=False, threaded=True)

if __name__ == '__main__':
    run_api()
