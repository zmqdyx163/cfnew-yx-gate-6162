#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
enable_workflow — 一键恢复 GitHub Actions 定时流水线
=====================================================
背景：GitHub 仓库 60 天无 commit 会自动把定时 workflow 置为 disabled_inactivity
（Actions 页面上需要手动点 enable）。本脚本用 API 实现同样操作：

  1. 检查 update.yml workflow 状态
  2. 若被暂停（disabled_inactivity / disabled_manually）→ 自动 enable
  3. 可选 --trigger：恢复后立即手动触发一次运行，确认链路可用

凭据来源：Windows 凭据管理器（git-credential-manager），无需在脚本里存 Token。
用法：
  python enable_workflow.py             # 仅检查/恢复
  python enable_workflow.py --trigger   # 检查/恢复 + 触发一次运行
"""

import base64
import hashlib
import json
import subprocess
import sys
import time
import urllib.request

# ---- 配置（部署时定死，改仓库时同步改这里） ----
REPO = "zmqdyx163/cfnew-yx-gate-6162"
WORKFLOW = "update.yml"
GCM = r"C:\Program Files\Git\mingw64\bin\git-credential-manager.exe"
GCM_USER = "zmqdyx163"
API = "https://api.github.com"


def get_token():
    """从 Windows 凭据管理器取 PAT（与 git push 同源）。"""
    out = subprocess.run(
        [GCM, "get"],
        input=f"protocol=https\nhost=github.com\nusername={GCM_USER}\n\n",
        capture_output=True, text=True, timeout=30,
    ).stdout
    for line in out.splitlines():
        if line.startswith("password="):
            return line[len("password="):]
    raise RuntimeError("凭据管理器里没有 zmqdyx163 的 GitHub 凭据，请先 git push 一次")


def api(method, path, token, body=None):
    req = urllib.request.Request(
        f"{API}/repos/{REPO}/{path}",
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Bearer {token}",
                 "Accept": "application/vnd.github+json"},
        method=method,
    )
    try:
        r = urllib.request.urlopen(req, timeout=30)
        t = r.read()
        return r.status, (json.loads(t) if t else {})
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def wait_run(token, minutes=8):
    """触发后等待最新一次 run 完成。"""
    for _ in range(minutes * 4):
        time.sleep(15)
        _, d = api("GET", "actions/runs?per_page=1", token)
        r = (d.get("workflow_runs") or [{}])[0]
        if r.get("status") == "completed":
            return r.get("conclusion")
        print(f"  运行中... ({r.get('status')})")
    return "timeout"


def main():
    trigger = "--trigger" in sys.argv
    token = get_token()

    # 1. 查状态
    s, wf = api("GET", f"actions/workflows/{WORKFLOW}", token)
    state = wf.get("state", "?")
    print(f"workflow 状态: {state}")

    # 2. 按需恢复
    if state == "active":
        print("✅ 定时流水线正常，无需恢复")
    elif state in ("disabled_inactivity", "disabled_manually", "disabled"):
        s, _ = api("PUT", f"actions/workflows/{WORKFLOW}/enable", token)
        print(f"已被 GitHub 暂停 → 一键恢复: HTTP {s} " + ("成功" if s in (200, 204) else "失败"))
        if s not in (200, 204):
            sys.exit(1)
    else:
        print(f"⚠️ 未知状态 {state}，请到 Actions 页面人工确认")

    # 3. 可选触发验证
    if trigger:
        s, _ = api("POST", f"actions/workflows/{WORKFLOW}/dispatches", token, {"ref": "main"})
        print(f"已手动触发一次运行: HTTP {s}")
        if s == 204:
            print("等待运行完成...")
            print(f"运行结果: {wait_run(token)}")
    else:
        print("提示: 加 --trigger 可在恢复后立即触发一次运行验证")


if __name__ == "__main__":
    main()
