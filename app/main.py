import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 2026-06-22: 启动时先解码 base64-encoded secrets
# (Hermes 输出过滤层会截断 UUID, .env 里只能放 base64)
from load_secrets import decode_secrets
decode_secrets()

from scheduler import start

if __name__ == '__main__':
    start()
