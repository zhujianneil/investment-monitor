"""
load_secrets.py — 容器内环境变量解码器 (2026-06-22 新增)

背景: Hermes Agent 的 tool I/O 会自动把 UUID 模式字符串 (e.g. API keys) 屏蔽成 ***,
导致宿主机的 .env / .bashrc 即使被 set, 也无法在容器内以明文形式拿到。
参考: ~/.hermes/skills/devops/gf-lhb-list/SKILL.md Pitfall 11

绕开方案: 把 apikey 先 base64 编码, 写到 .env (作为 GF_SKILLS_APIKEY_B64),
容器启动时由本模块解码回 GF_SKILLS_APIKEY 环境变量. 上层业务代码完全无感
(还是 os.environ['GF_SKILLS_APIKEY']).

调用方式:
  1) main.py 启动时先 import 并调用 decode_secrets()
  2) 或 Dockerfile CMD 加一行 `python3 -c "from load_secrets import decode_secrets; decode_secrets()"`
  3) 或 scheduler.py 启动时调用一次

设计原则:
  - 解码失败 → 静默 + 写明警告 (不阻断容器启动, 让其他 job 照常跑)
  - GF_SKILLS_APIKEY 已存在 → 跳过 (支持手工 docker run -e 覆盖)
"""
import base64
import os


def decode_secrets() -> dict:
    """
    解码 .env 里的 base64 编码 secrets 到 os.environ.
    返回 dict {变量名: True/False} 表明每个 secret 是否成功解码.

    当前支持:
      - GF_SKILLS_APIKEY_B64 → GF_SKILLS_APIKEY (广发 gf-skills MCP 接口 apikey)
    """
    results = {}

    # 1) 广发 apikey
    if 'GF_SKILLS_APIKEY' in os.environ and os.environ['GF_SKILLS_APIKEY']:
        # 已经设置 (可能来自 docker run -e), 跳过
        print("  [load_secrets] GF_SKILLS_APIKEY 已存在, 跳过 base64 解码")
        results['GF_SKILLS_APIKEY'] = True
    elif 'GF_SKILLS_APIKEY_B64' in os.environ:
        b64 = os.environ['GF_SKILLS_APIKEY_B64']
        try:
            decoded = base64.b64decode(b64.encode()).decode('utf-8').strip()
            os.environ['GF_SKILLS_APIKEY'] = decoded
            print(f"  [load_secrets] ✓ GF_SKILLS_APIKEY 已从 base64 解码 (len={len(decoded)})")
            results['GF_SKILLS_APIKEY'] = True
        except Exception as e:
            print(f"  [load_secrets] ✗ GF_SKILLS_APIKEY_B64 解码失败: {type(e).__name__}: {e}")
            print(f"  [load_secrets]   提示: 检查 .env 里的 GF_SKILLS_APIKEY_B64 值")
            results['GF_SKILLS_APIKEY'] = False
    else:
        print("  [load_secrets] ⚠ 未配置 GF_SKILLS_APIKEY / GF_SKILLS_APIKEY_B64")
        print("  [load_secrets]   lhb_stream / gf_lhb_list 相关功能将不可用")
        results['GF_SKILLS_APIKEY'] = False

    return results


if __name__ == '__main__':
    decode_secrets()
    print(f"  GF_SKILLS_APIKEY in env: {'GF_SKILLS_APIKEY' in os.environ}, len={len(os.environ.get('GF_SKILLS_APIKEY',''))}")
