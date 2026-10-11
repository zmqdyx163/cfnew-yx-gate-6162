# DEPLOY.md — cfnew-yx-gate 部署指令书（供自动化 Agent 直接执行）

> 本文档是自包含的：**全部源码内嵌在下方**，无需访问任何外部链接即可完成部署。
> 目标读者：具备 shell / GitHub 操作能力的自动化 Agent（或人类）。
> 生成时间：2026-10-11

---

## 0. 项目概述（30 秒读懂）

**cfnew-yx-gate** 是一个为 [byJoey/cfnew](https://github.com/byJoey/cfnew) 自动供给优选 IP
与增强家宽订阅的 GitHub Actions 流水线（思路源自 heleihub/gate，适配 Cloudflare 免费计划，
零 Worker 依赖）：

- **优选IP流水线**：每 30 分钟拉取公开优选 API → GitHub Runner 三段验证
  （TCP+TLS+/cdn-cgi/trace）→ colo 标注排序 → 发布 `nodes.txt` 到 GitHub Pages；
  CFnew 面板 `yxURL` 填一次即永久自动更新
- **（可选）CLASH 家宽增强模块 `jk_builder`**：1:1 保留 CFnew v3.1 原版家宽链式的
  **全部功能**（VPN Gate 节点源与机房剔除规则、速度倒序、🏠 国家-家宽-NN 命名、
  ca/cert/key YAML 锚点、⚡CF前置(url-test)/🏠家宽自动(fallback)/🏠家宽节点(select)/
  🚀节点选择 四分组、TLS-only 前置、无前置退化直连、fake-ip DNS + GEOIP 规则、
  失败沿用上一版），额外增加 **Runner 侧 TCP 存活预验证**，死节点不进订阅
- 原版 CFnew 的 `?target=vg` 原生家宽完全不受影响，且 yxURL 优选节点会自动成为其前置

**部署只涉及 GitHub，不触碰 Cloudflare**（CFnew 假定已部署好；如未部署，见附录 B）。

---

## 1. 前置条件检查

| 检查项 | 要求 | 验证命令 |
| :-- | :-- | :-- |
| GitHub CLI 已登录 | `gh auth status` 正常 | `gh auth status` |
| 或者有 GitHub Token | scopes: `repo`, `workflow` | `echo $GITHUB_TOKEN` |
| git 可用 | 任意近期版本 | `git --version` |
| 目标 CFnew 实例 | 已部署、能打开面板（第 4.4 步起需要） | 浏览器访问 |

---

## 2. 创建项目文件

在空目录中按以下文件树创建文件（内容见第 3 节，**逐字复制，不要改动**）：

```
cfnew-yx-gate/
├── pipeline.py                    # 核心流水线：优选IP 收集/验证/发布
├── jk_builder.py                  # 可选：CLASH 家宽订阅增强构建器
├── requirements.txt
├── README.md
├── web/
│   └── index.html                 # 仪表盘模板
└── .github/
    └── workflows/
        └── update.yml             # 定时任务 + Pages 发布
```

---

## 3. 源码（全文件内嵌）


### 文件: `pipeline.py`

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cfnew-yx-gate — CFnew 优选IP全自动流水线
==========================================
思路借鉴 heleihub/gate（GitHub Actions 定时流水线 → 节点验证 → GitHub Pages 发布 →
面板填一次 URL 永久生效），但针对 byJoey/cfnew 全新实现，且完全适配 Cloudflare 免费计划：

  gate 的验证依赖一个额外部署的检测 Worker（占用 Workers 免费额度）；
  本项目把验证搬到 GitHub Runner 本机直接完成（TCP 连通 + TLS 握手 + /cdn-cgi/trace
  回显三段验证），不需要部署任何额外 Worker —— Cloudflare 侧零资源占用。

流程:
  1. 从多个公开优选 API 拉取候选 Cloudflare 入口 IP（电信/联通/移动/BestCF 等）
  2. 合并静态优选域名池；候选不足时从 Cloudflare 官方网段随机采样补足
  3. 并发三段验证: TCP 连通 → TLS 握手(SNI) → GET /cdn-cgi/trace 确认真实 CF 边缘
  4. 提取 colo 机房码 → 映射国旗/中文地名 → 按延迟排序 → 截取 TOP N
  5. 生成 public/nodes.txt（CFnew yxURL 格式: 每行 `IP:端口#名称`）
     + public/data.json + public/index.html 仪表盘
  6. (可选) 通过 CFnew 的 /{UUID}/api/preferred-ips API 直接推送到你的实例

CFnew 侧只需在图形面板「优选IP来源URL (yxURL)」里填一次:
  https://<你的GitHub用户名>.github.io/<仓库名>/nodes.txt
之后每次刷新订阅，CFnew 都会重新拉取本列表，节点自动更新，零手动。
"""

import ipaddress
import json
import os
import random
import re
import socket
import ssl
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ---------------------------------------------------------------------------
# 配置（全部可用环境变量覆盖）
# ---------------------------------------------------------------------------
REPO_DIR = os.path.dirname(os.path.abspath(__file__))
PUBLIC_DIR = os.environ.get("PUBLIC_DIR", os.path.join(REPO_DIR, "public"))
TEMPLATE_HTML = os.path.join(REPO_DIR, "web", "index.html")

# 候选 IP 来源（逗号分隔，返回内容须是每行 `IP:端口#备注` 或纯 `IP`）
IP_SOURCES = [u.strip() for u in os.environ.get(
    "IP_SOURCES",
    "https://cf.090227.xyz/ct?ips=16&port=443,"
    "https://cf.090227.xyz/cu?ips=16&port=443,"
    "https://cf.090227.xyz/cm?ips=16&port=443,"
    "https://ipdb.api.030101.xyz/?type=bestcf",
).split(",") if u.strip()]

# 来源 URL → 名称标签（命中子串即匹配）
SOURCE_TAG_RULES = [
    ("/ct", "电信"), ("/cu", "联通"), ("/cm", "移动"),
    ("bestcf", "BestCF"), ("bestproxy", "BestProxy"),
]

# 静态优选域名池（CFnew 会把非 IP 行当「优选域名」处理）
EDGE_DOMAINS = [d.strip() for d in os.environ.get(
    "EDGE_DOMAINS",
    "www.visa.com.sg,www.visa.com.hk,www.wto.org,www.shopify.com,time.is,icook.hk,japan.com",
).split(",") if d.strip()]

# 候选 IP 总数不足 MIN_CANDIDATES 时，从官方网段随机采样 RANDOM_SAMPLE 个补足
MIN_CANDIDATES = int(os.environ.get("MIN_CANDIDATES", "12"))
RANDOM_SAMPLE = int(os.environ.get("RANDOM_SAMPLE", "32"))
CF_RANGES = [
    "104.16.0.0/13", "172.64.0.0/14", "162.159.0.0/16",
    "188.114.96.0/20", "190.93.240.0/20", "141.101.64.0/18", "108.162.192.0/18",
]

MAX_NODES = int(os.environ.get("MAX_NODES", "60"))           # nodes.txt 中 IP 节点上限
CONCURRENCY = max(1, int(os.environ.get("CHECK_CONCURRENCY", "32")))
CONNECT_TIMEOUT = float(os.environ.get("CONNECT_TIMEOUT", "6"))
TRACE_HOST = os.environ.get("TRACE_HOST", "www.cloudflare.com")  # SNI / Host
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "20"))

# 可选: 直接推送到 CFnew 实例（需 CFnew 面板里开启「允许API管理」ae=yes）
CFNEW_BASE = os.environ.get("CFNEW_BASE", "").strip().rstrip("/")   # 如 https://xxx.pages.dev
CFNEW_UUID = os.environ.get("CFNEW_UUID", "").strip()               # UUID 或自定义路径(d变量)
CFNEW_PUSH_MODE = os.environ.get("CFNEW_PUSH_MODE", "replace")      # replace | append
CFNEW_PUSH_LIMIT = int(os.environ.get("CFNEW_PUSH_LIMIT", "30"))

# ---------------------------------------------------------------------------
# Cloudflare colo(IATA) → (国旗, 中文地名)
# ---------------------------------------------------------------------------
COLO_MAP = {
    # 美洲
    "LAX": ("🇺🇸", "洛杉矶"), "SJC": ("🇺🇸", "圣何塞"), "SEA": ("🇺🇸", "西雅图"),
    "PDX": ("🇺🇸", "波特兰"), "SFO": ("🇺🇸", "旧金山"), "ORD": ("🇺🇸", "芝加哥"),
    "DFW": ("🇺🇸", "达拉斯"), "IAD": ("🇺🇸", "华盛顿"), "EWR": ("🇺🇸", "纽瓦克"),
    "ATL": ("🇺🇸", "亚特兰大"), "MIA": ("🇺🇸", "迈阿密"), "DEN": ("🇺🇸", "丹佛"),
    "PHX": ("🇺🇸", "凤凰城"), "MCI": ("🇺🇸", "堪萨斯城"), "BOS": ("🇺🇸", "波士顿"),
    "BUF": ("🇺🇸", "布法罗"), "CLT": ("🇺🇸", "夏洛特"), "IAH": ("🇺🇸", "休斯顿"),
    "LAS": ("🇺🇸", "拉斯维加斯"), "MSP": ("🇺🇸", "明尼阿波利斯"), "PHL": ("🇺🇸", "费城"),
    "PIT": ("🇺🇸", "匹兹堡"), "SLC": ("🇺🇸", "盐湖城"), "STL": ("🇺🇸", "圣路易斯"),
    "TPA": ("🇺🇸", "坦帕"), "HNL": ("🇺🇸", "檀香山"),
    "YVR": ("🇨🇦", "温哥华"), "YYZ": ("🇨🇦", "多伦多"), "YUL": ("🇨🇦", "蒙特利尔"),
    "GRU": ("🇧🇷", "圣保罗"), "GIG": ("🇧🇷", "里约"), "EZE": ("🇦🇷", "布宜诺斯艾利斯"),
    "SCL": ("🇨🇱", "圣地亚哥"), "BOG": ("🇨🇴", "波哥大"), "LIM": ("🇵🇪", "利马"),
    "MEX": ("🇲🇽", "墨西哥城"), "QRO": ("🇲🇽", "克雷塔罗"),
    # 亚太
    "NRT": ("🇯🇵", "东京"), "KIX": ("🇯🇵", "大阪"), "FUK": ("🇯🇵", "福冈"),
    "OKA": ("🇯🇵", "冲绳"), "HKG": ("🇭🇰", "香港"), "SIN": ("🇸🇬", "新加坡"),
    "ICN": ("🇰🇷", "首尔"), "TPE": ("🇹🇼", "台北"), "KHH": ("🇹🇼", "高雄"),
    "BOM": ("🇮🇳", "孟买"), "DEL": ("🇮🇳", "新德里"), "MAA": ("🇮🇳", "金奈"),
    "BLR": ("🇮🇳", "班加罗尔"), "SYD": ("🇦🇺", "悉尼"), "MEL": ("🇦🇺", "墨尔本"),
    "BNE": ("🇦🇺", "布里斯班"), "PER": ("🇦🇺", "珀斯"), "ADL": ("🇦🇺", "阿德莱德"),
    "AKL": ("🇳🇿", "奥克兰"), "BKK": ("🇹🇭", "曼谷"), "KUL": ("🇲🇾", "吉隆坡"),
    "CGK": ("🇮🇩", "雅加达"), "MNL": ("🇵🇭", "马尼拉"), "SGN": ("🇻🇳", "胡志明"),
    "HAN": ("🇻🇳", "河内"), "PNH": ("🇰🇭", "金边"), "KTM": ("🇳🇵", "加德满都"),
    "CMB": ("🇱🇰", "科伦坡"), "DAC": ("🇧🇩", "达卡"), "ULN": ("🇲🇳", "乌兰巴托"),
    # 欧洲
    "LHR": ("🇬🇧", "伦敦"), "MAN": ("🇬🇧", "曼彻斯特"), "EDI": ("🇬🇧", "爱丁堡"),
    "CDG": ("🇫🇷", "巴黎"), "MRS": ("🇫🇷", "马赛"), "FRA": ("🇩🇪", "法兰克福"),
    "MUC": ("🇩🇪", "慕尼黑"), "DUS": ("🇩🇪", "杜塞尔多夫"), "HAM": ("🇩🇪", "汉堡"),
    "TXL": ("🇩🇪", "柏林"), "AMS": ("🇳🇱", "阿姆斯特丹"), "MAD": ("🇪🇸", "马德里"),
    "BCN": ("🇪🇸", "巴塞罗那"), "MXP": ("🇮🇹", "米兰"), "FCO": ("🇮🇹", "罗马"),
    "ZRH": ("🇨🇭", "苏黎世"), "GVA": ("🇨🇭", "日内瓦"), "ARN": ("🇸🇪", "斯德哥尔摩"),
    "CPH": ("🇩🇰", "哥本哈根"), "OSL": ("🇳🇴", "奥斯陆"), "HEL": ("🇫🇮", "赫尔辛基"),
    "WAW": ("🇵🇱", "华沙"), "VIE": ("🇦🇹", "维也纳"), "PRG": ("🇨🇿", "布拉格"),
    "BUD": ("🇭🇺", "布达佩斯"), "OTP": ("🇷🇴", "布加勒斯特"), "SOF": ("🇧🇬", "索菲亚"),
    "ATH": ("🇬🇷", "雅典"), "BRU": ("🇧🇪", "布鲁塞尔"), "DUB": ("🇮🇪", "都柏林"),
    "LIS": ("🇵🇹", "里斯本"), "IST": ("🇹🇷", "伊斯坦布尔"), "KBP": ("🇺🇦", "基辅"),
    "DME": ("🇷🇺", "莫斯科"), "LED": ("🇷🇺", "圣彼得堡"),
    # 中东/非洲
    "DXB": ("🇦🇪", "迪拜"), "DOH": ("🇶🇦", "多哈"), "TLV": ("🇮🇱", "特拉维夫"),
    "RUH": ("🇸🇦", "利雅得"), "JED": ("🇸🇦", "吉达"), "BAH": ("🇧🇭", "巴林"),
    "JNB": ("🇿🇦", "约翰内斯堡"), "CPT": ("🇿🇦", "开普敦"), "NBO": ("🇰🇪", "内罗毕"),
    "CAI": ("🇪🇬", "开罗"), "LOS": ("🇳🇬", "拉各斯"),
}

# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------
_section = None

def log(section, msg=""):
    global _section
    if section != _section:
        print(f"========== {section} ==========")
        _section = section
    if msg:
        print(msg, flush=True)

# ---------------------------------------------------------------------------
# 候选收集
# ---------------------------------------------------------------------------
_IP_RE = re.compile(r"^(?:\d{1,3}\.){3}\d{1,3}$")

def source_tag(url):
    u = url.lower()
    for key, tag in SOURCE_TAG_RULES:
        if key in u:
            return tag
    return "优选"

def parse_source_lines(text, default_port, tag):
    """解析每行 `IP[:端口][#备注]`，返回候选 dict 列表。"""
    out = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("//"):
            continue
        endpoint = line.split("#")[0].strip()
        if not endpoint:
            continue
        if ":" in endpoint and not endpoint.startswith("["):
            host, _, port_s = endpoint.rpartition(":")
            port = int(port_s) if port_s.isdigit() else default_port
        else:
            host, port = endpoint, default_port
        host = host.strip().strip("[]")
        if not _IP_RE.match(host):
            continue  # 本函数只收 IPv4；域名走 EDGE_DOMAINS
        out.append({"host": host, "port": port, "tag": tag, "type": "ip"})
    return out

def fetch_candidates():
    candidates = []
    for url in IP_SOURCES:
        tag = source_tag(url)
        try:
            log("IP 来源", f"请求: {url}")
            resp = requests.get(url, timeout=HTTP_TIMEOUT,
                                headers={"User-Agent": "Mozilla/5.0 (cfnew-yx-gate)"})
            if resp.status_code != 200:
                log("IP 来源", f"  状态码异常: {resp.status_code}")
                continue
            got = parse_source_lines(resp.text, 443, tag)
            log("IP 来源", f"  解析到 {len(got)} 个候选 ({tag})")
            candidates.extend(got)
        except Exception as exc:
            log("IP 来源", f"  失败: {type(exc).__name__}: {exc}")
    return candidates

def random_cf_ips(n):
    """从 Cloudflare 官方网段随机采样 n 个 IP 作为候选兜底。"""
    out = []
    nets = [ipaddress.ip_network(c) for c in CF_RANGES]
    weights = [net.num_addresses for net in nets]
    for _ in range(n):
        net = random.choices(nets, weights=weights, k=1)[0]
        ip = ipaddress.ip_address(
            int(net.network_address) + random.randrange(net.num_addresses))
        out.append({"host": str(ip), "port": 443, "tag": "随机", "type": "ip"})
    return out

def dedupe(cands):
    seen, out = set(), []
    for c in cands:
        key = (c["host"], c["port"])
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    return out

# ---------------------------------------------------------------------------
# 三段验证: TCP → TLS(SNI) → /cdn-cgi/trace
# ---------------------------------------------------------------------------
def check_one(cand):
    """返回验证结果 dict。成功的节点带 latency_ms + colo。"""
    out = dict(cand)
    out["success"] = False
    out["colo"] = None
    out["latency_ms"] = None
    out["error"] = None
    host, port = cand["host"], cand["port"]
    t0 = time.monotonic()
    try:
        raw = socket.create_connection((host, port), timeout=CONNECT_TIMEOUT)
    except Exception as exc:
        out["error"] = f"tcp: {type(exc).__name__}"
        return out
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        raw.settimeout(CONNECT_TIMEOUT)
        tls = ctx.wrap_socket(raw, server_hostname=TRACE_HOST)
        req = (f"GET /cdn-cgi/trace HTTP/1.1\r\n"
               f"Host: {TRACE_HOST}\r\n"
               f"User-Agent: cfnew-yx-gate\r\n"
               f"Connection: close\r\n\r\n").encode()
        tls.sendall(req)
        buf = b""
        while len(buf) < 65536:
            try:
                chunk = tls.recv(4096)
            except Exception:
                break
            if not chunk:
                break
            buf += chunk
        tls.close()
        text = buf.decode("utf-8", "replace")
        m = re.search(r"^colo=([A-Z]{3})", text, re.M)
        if not m:
            out["error"] = "trace: no colo (不是 CF 边缘?)"
            return out
        out["colo"] = m.group(1)
        out["latency_ms"] = round((time.monotonic() - t0) * 1000)
        out["success"] = True
        return out
    except Exception as exc:
        out["error"] = f"tls/http: {type(exc).__name__}"
        try:
            raw.close()
        except Exception:
            pass
        return out

def check_all(cands):
    results = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futs = [pool.submit(check_one, c) for c in cands]
        for fut in as_completed(futs):
            r = fut.result()
            mark = "✓" if r["success"] else "✗"
            extra = (f"{r['colo']} {r['latency_ms']}ms" if r["success"] else r["error"])
            log("验证", f"  {mark} {r['host']}:{r['port']} [{r['tag']}] {extra}")
            results.append(r)
    return results

# ---------------------------------------------------------------------------
# 命名 + 输出
# ---------------------------------------------------------------------------
def colo_label(colo):
    flag, name = COLO_MAP.get(colo or "", ("🌐", colo or "未知"))
    return flag, name

def build_named_nodes(ok_ips, ok_domains):
    """生成 CFnew 友好的节点名并排序。"""
    ok_ips.sort(key=lambda r: (r["latency_ms"] is None, r["latency_ms"] or 0))
    counters = {}
    ip_nodes = []
    for r in ok_ips[:MAX_NODES]:
        flag, name = colo_label(r["colo"])
        base = f"{flag}{name}-{r['colo']}-{r['tag']}"
        counters[base] = counters.get(base, 0) + 1
        node_name = f"{base}-{counters[base]:02d}"
        ip_nodes.append({
            "ip": r["host"], "port": r["port"], "name": node_name,
            "colo": r["colo"], "latency_ms": r["latency_ms"], "tag": r["tag"],
        })
    dom_nodes = []
    for i, r in enumerate(ok_domains, 1):
        flag, name = colo_label(r["colo"])
        dom_nodes.append({
            "ip": r["host"], "port": r["port"],
            "name": f"优选域名-{r['host']}",
            "colo": r["colo"], "latency_ms": r["latency_ms"], "tag": "域名",
        })
    return ip_nodes, dom_nodes

def write_outputs(ip_nodes, dom_nodes, stats):
    os.makedirs(PUBLIC_DIR, exist_ok=True)

    # 1) nodes.txt — CFnew yxURL 直接可用: 每行 `IP:端口#名称`
    lines = [f"{n['ip']}:{n['port']}#{n['name']}" for n in ip_nodes]
    lines += [f"{n['ip']}:{n['port']}#{n['name']}" for n in dom_nodes]
    nodes_txt = "\n".join(lines) + "\n"
    with open(os.path.join(PUBLIC_DIR, "nodes.txt"), "w", encoding="utf-8") as f:
        f.write(nodes_txt)
    log("输出", f"nodes.txt 写入 {len(lines)} 行")

    # 2) data.json — 仪表盘数据
    data = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "stats": stats,
        "ip_nodes": ip_nodes,
        "domain_nodes": dom_nodes,
    }
    with open(os.path.join(PUBLIC_DIR, "data.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)

    # 3) index.html — 模板替换
    try:
        with open(TEMPLATE_HTML, encoding="utf-8") as f:
            html = f.read()
        html = html.replace("__DATA_JSON__", json.dumps(data, ensure_ascii=False))
        with open(os.path.join(PUBLIC_DIR, "index.html"), "w", encoding="utf-8") as f:
            f.write(html)
        log("输出", "index.html 仪表盘已生成")
    except FileNotFoundError:
        log("输出", "web/index.html 模板缺失, 跳过仪表盘")
    return data

# ---------------------------------------------------------------------------
# 可选: 推送到 CFnew preferred-ips API
# ---------------------------------------------------------------------------
def push_to_cfnew(ip_nodes):
    if not (CFNEW_BASE and CFNEW_UUID):
        log("API 推送", "未配置 CFNEW_BASE / CFNEW_UUID, 跳过（仅发布 yxURL 列表）")
        return
    api = f"{CFNEW_BASE}/{CFNEW_UUID}/api/preferred-ips"
    payload = [{"ip": n["ip"], "port": n["port"], "name": n["name"]}
               for n in ip_nodes[:CFNEW_PUSH_LIMIT]]
    if not payload:
        log("API 推送", "无可推送节点, 跳过")
        return
    try:
        if CFNEW_PUSH_MODE == "replace":
            r = requests.delete(api, json={"all": True}, timeout=HTTP_TIMEOUT)
            log("API 推送", f"清空旧列表: HTTP {r.status_code}")
        r = requests.post(api, json=payload, timeout=HTTP_TIMEOUT,
                          headers={"Content-Type": "application/json"})
        log("API 推送", f"推送 {len(payload)} 个节点: HTTP {r.status_code} {r.text[:200]}")
        if r.status_code != 200:
            log("API 推送", "提示: 需在 CFnew 面板开启「允许API管理」(ae=yes)")
    except Exception as exc:
        log("API 推送", f"失败(不影响发布): {type(exc).__name__}: {exc}")

# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def main():
    t_start = time.monotonic()

    # 1. 收集候选
    cands = fetch_candidates()
    cands = dedupe(cands)
    if len(cands) < MIN_CANDIDATES:
        log("兜底", f"候选仅 {len(cands)} 个 (<{MIN_CANDIDATES}), "
                    f"从官方网段随机补 {RANDOM_SAMPLE} 个")
        cands = dedupe(cands + random_cf_ips(RANDOM_SAMPLE))
    log("候选", f"共 {len(cands)} 个 IP 候选, {len(EDGE_DOMAINS)} 个优选域名")

    # 2. 验证 IP
    results = check_all(cands)
    ok_ips = [r for r in results if r["success"]]

    # 3. 验证域名
    dom_cands = [{"host": d, "port": 443, "tag": "域名", "type": "domain"}
                 for d in EDGE_DOMAINS]
    dom_results = check_all(dom_cands) if dom_cands else []
    ok_domains = [r for r in dom_results if r["success"]]
    ok_domains.sort(key=lambda r: (r["latency_ms"] is None, r["latency_ms"] or 0))

    # 4. 命名 + 输出
    ip_nodes, dom_nodes = build_named_nodes(ok_ips, ok_domains)
    stats = {
        "candidates": len(cands),
        "checked": len(results) + len(dom_results),
        "ip_ok": len(ok_ips),
        "ip_published": len(ip_nodes),
        "domain_ok": len(ok_domains),
        "colos": len({n["colo"] for n in ip_nodes}),
        "duration_s": round(time.monotonic() - t_start, 1),
        "note": "延迟为 GitHub Runner 测得的 RTT, 仅代表节点存活与边缘可达, 不等于你本地延迟",
    }
    write_outputs(ip_nodes, dom_nodes, stats)

    # 5. 可选 API 推送
    push_to_cfnew(ip_nodes)

    # 6. 可选: CLASH 家宽订阅增强构建（1:1 保留原版全部功能 + 存活预验证）
    try:
        import jk_builder
        jk_stats = jk_builder.run(PUBLIC_DIR)
        if jk_stats:
            stats["jk"] = jk_stats
            # 家宽统计并入 data.json（不含 token, 不泄露订阅路径）
            write_outputs(ip_nodes, dom_nodes, stats)
    except Exception as exc:
        log("家宽", f"模块异常(不影响主流程): {type(exc).__name__}: {exc}")

    log("完成", f"IP {len(ip_nodes)} 个 / 域名 {len(dom_nodes)} 个, "
               f"耗时 {stats['duration_s']}s")
    if not ip_nodes and not dom_nodes:
        log("完成", "警告: 本轮没有任何节点通过验证, nodes.txt 为空")
        # 不以非零码退出: 保持 Pages 上一版内容不被失败中断（部署空文件也可由 CFnew 内置优选兜底）

if __name__ == "__main__":
    main()
```


### 文件: `jk_builder.py`

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
jk_builder — CLASH 家宽订阅增强构建器（可选模块）
====================================================
1:1 复刻 byJoey/cfnew v3.1「家宽链式」(target=vg) 生成的 Clash 订阅的**全部功能**：

  ✔ 同样的节点来源: VPN Gate api/iphone (https → http 回退, 另加 GitHub 镜像兜底)
  ✔ 同样的筛选: 剔除 public-vpn* 自营机房 / 219.100.37.* 网段 / 非 TCP / 短配置
  ✔ 同样的排序: 按 VPN Gate 速度字段倒序（原版注释: 按会话数挑效果更差）
  ✔ 同样的命名: 🏠 {国家}-家宽-{序号}（按国家独立计数）
  ✔ 同样的证书处理: 全站共用一份 ca/cert/key, YAML 锚点 &jkca/&jkcert/&jkkey 省几百 KB
  ✔ 同样的节点参数: type openvpn / proto tcp / username+password vpn / udp false
                    / handshake-timeout 30 / remote-dns-resolve / dns [8.8.8.8, 1.1.1.1]
  ✔ 同样的前置逻辑: 只取 vless/trojan; 有 TLS 节点就只用 TLS（明文裸奔会暴露握手特征）;
                    一个前置都没有就退化为直连落地（订阅至少能用）
  ✔ 同样的分组: ⚡ CF前置(url-test 300s) / 🏠 家宽自动(fallback 1800s lazy)
               / 🏠 家宽节点(select 按国家排) / 🚀 节点选择(含 DIRECT)
  ✔ 同样的 DNS / 规则: fake-ip + GEOIP,LAN/CN 直连 + MATCH
  ✔ 同样的容错语义: 原版拉不到节点回 503 让客户端沿用旧配置；
                    本模块失败时自动回取 Pages 上一版文件重新发布, 不会用空配置覆盖

在原版之上的增强（这正是 gate 思路的价值）:
  ★ 发布前在 GitHub Runner 对每个家宽节点做 TCP 存活预验证, 死节点不进订阅
    （原版实测约一半连不上, 预验证后客户端 fallback 组收敛更快）
  ★ 前置节点直接复用你 CFnew 实例的真实订阅（含 yxURL 喂进去的优选 IP）

注意: 原版 CFnew 的 `?target=vg` 原生功能不受本模块任何影响, 两者可并存。
隐私提示: 生成的 clash.yaml 含你的 UUID, 发布路径带不可猜测 token, 请勿外传。
"""

import base64
import hashlib
import os
import re
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# 配置（与原版一致的常量 + 模块开关）
# ---------------------------------------------------------------------------
JK_ENABLE = os.environ.get("JK_ENABLE", "").strip().lower() in ("yes", "true", "1")
CFNEW_BASE = os.environ.get("CFNEW_BASE", "").strip().rstrip("/")
CFNEW_UUID = os.environ.get("CFNEW_UUID", "").strip()
JK_TOKEN = os.environ.get("JK_TOKEN", "").strip()

VG_SOURCE = "https://www.vpngate.net/api/iphone/"           # 原版同款主源
VG_MIRROR = ("https://raw.githubusercontent.com/fdciabdul/"
             "Vpngate-Scraper-API/main/json/data.json")      # gate 同款镜像兜底
DC_HOST_PREFIX = "public-vpn"                                 # 原版: 剔除自营机房
DC_IP_PREFIX = "219.100.37."                                  # 原版: 剔除机房网段
CFG_HEAD_LEN = 6000                                           # 原版: 先解前段提速

ALIVE_TIMEOUT = float(os.environ.get("JK_ALIVE_TIMEOUT", "6"))
ALIVE_CONCURRENCY = int(os.environ.get("JK_ALIVE_CONCURRENCY", "32"))
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "25"))
MAX_JK_NODES = int(os.environ.get("JK_MAX_NODES", "0"))       # 0 = 不限(原版为全量)

GROUP_FRONT = "⚡ CF前置"
GROUP_AUTO = "🏠 家宽自动"
GROUP_PICK = "🏠 家宽节点"
GROUP_MAIN = "🚀 节点选择"

def log(msg):
    print(f"[家宽] {msg}", flush=True)

def yq(s):
    """YAML 双引号安全转义。"""
    return '"' + str(s).replace("\\", "\\\\").replace('"', '\\"') + '"'

# ---------------------------------------------------------------------------
# VPN Gate 清单解析（逐条对齐原版 解析家宽清单）
# ---------------------------------------------------------------------------
def _b64(s, head_only=False):
    clean = re.sub(r"\s+", "", s or "")
    if head_only and len(clean) > CFG_HEAD_LEN:
        clean = clean[:CFG_HEAD_LEN]
    clean = clean[: len(clean) - len(clean) % 4]
    text = base64.b64decode(clean, validate=False).decode("utf-8", "replace")
    # JS 多行 $ 把 \r 视作行终止符而 Python 不会, 统一换行以对齐原版解析行为
    return text.replace("\r\n", "\n").replace("\r", "\n")

def _directive(cfg, name):
    m = re.search(rf"^[ \t]*{name}[ \t]+(.+?)[ \t]*$", cfg, re.M)
    return m.group(1) if m else ""

def _inline_block(cfg, tag):
    m = re.search(rf"<{tag}>\s*([\s\S]*?)\s*</{tag}>", cfg)
    return m.group(1).strip() if m else ""

def parse_vg_csv(text):
    """与原版相同的筛选: 机房剔除 / 速度倒序 / 仅 TCP / 共用证书。"""
    cands = []
    for raw in text.split("\n"):
        line = raw.strip()
        if not line or line[0] in "*#":
            continue
        f = line.split(",")
        if len(f) < 15:
            continue
        if (f[0] or "").startswith(DC_HOST_PREFIX):
            continue
        if (f[1] or "").startswith(DC_IP_PREFIX):
            continue
        cfg64 = f[-1]
        if not cfg64 or len(cfg64) < 100:
            continue
        try:
            speed = int(f[4])
        except (ValueError, IndexError):
            speed = 0
        cands.append({"country": (f[6] or "").upper() or "XX",
                      "speed": speed, "cfg64": cfg64})
    cands.sort(key=lambda c: -c["speed"])   # 原版: 按速度倒序, 别按会话数挑
    nodes, cert = [], None
    for c in cands:
        try:
            cfg = _b64(c["cfg64"], head_only=True)
            if not _directive(cfg, "remote"):
                cfg = _b64(c["cfg64"])
        except Exception:
            continue
        if (_directive(cfg, "proto") or "tcp").lower() != "tcp":
            continue  # 原版: 前置只能承载 TCP
        remote = _directive(cfg, "remote").split()
        if not remote:
            continue
        if cert is None:
            try:
                full = _b64(c["cfg64"])
                got = {t: _inline_block(full, t) for t in ("ca", "cert", "key")}
                if all(got.values()):
                    cert = got
            except Exception:
                pass
            if cert is None:
                continue  # 原版: 证书没拿到之前的节点跳过
        nodes.append({
            "country": c["country"],
            "host": remote[0],
            "port": int(remote[1]) if len(remote) > 1 and remote[1].isdigit() else 443,
            "cipher": _directive(cfg, "cipher") or "AES-128-CBC",
            "auth": _directive(cfg, "auth") or "SHA1",
        })
    return nodes, cert

def parse_vg_mirror(data):
    """镜像 JSON → 伪 CSV 行复用同一解析器。"""
    servers = []
    items = data if isinstance(data, list) else [data]
    for it in items:
        if isinstance(it, dict) and isinstance(it.get("servers"), list):
            servers.extend(it["servers"])
        elif isinstance(it, dict):
            servers.append(it)
    lines = []
    for s in servers:
        host = str(s.get("hostname") or s.get("host") or "")
        ip = str(s.get("ip") or "")
        cc = str(s.get("countryshort") or s.get("country_short") or "")
        speed = str(s.get("speed") or 0)
        cfg = str(s.get("openvpn_configdata_base64") or s.get("config_b64") or "")
        if not host or not cfg:
            continue
        # 列位对齐: 0=host 1=ip 4=speed 6=countryshort 14=占位 last=cfg64
        lines.append(",".join([host, ip, "", "", speed, "", cc,
                               "", "", "", "", "", "", "", cfg]))
    return "\n".join(lines)

def fetch_vg_nodes():
    for src in (VG_SOURCE, VG_SOURCE.replace("https:", "http:", 1)):
        try:
            log(f"拉取节点源: {src}")
            r = requests.get(src, timeout=HTTP_TIMEOUT,
                             headers={"User-Agent": "Mozilla/5.0", "Accept": "text/plain"})
            if r.status_code == 200 and r.text:
                nodes, cert = parse_vg_csv(r.text)
                if nodes and cert:
                    return nodes, cert
        except Exception as exc:
            log(f"节点源失败: {type(exc).__name__}")
    try:
        log(f"回退镜像: {VG_MIRROR}")
        r = requests.get(VG_MIRROR, timeout=HTTP_TIMEOUT,
                         headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        nodes, cert = parse_vg_csv(parse_vg_mirror(r.json()))
        if nodes and cert:
            return nodes, cert
    except Exception as exc:
        log(f"镜像也失败: {type(exc).__name__}")
    raise RuntimeError("VPN Gate 主源/明文回退/镜像全部失败")

# ---------------------------------------------------------------------------
# 增强: Runner 侧 TCP 存活预验证（原版没有, 这是 gate 思路的核心价值）
# ---------------------------------------------------------------------------
def alive_filter(nodes):
    def probe(n):
        t0 = time.monotonic()
        try:
            s = socket.create_connection((n["host"], n["port"]), timeout=ALIVE_TIMEOUT)
            s.close()
            n["rtt"] = round((time.monotonic() - t0) * 1000)
            return n
        except Exception:
            return None
    alive = []
    with ThreadPoolExecutor(max_workers=ALIVE_CONCURRENCY) as pool:
        futs = {pool.submit(probe, n): n for n in nodes}
        for fut in as_completed(futs):
            r = fut.result()
            if r:
                alive.append(r)
    # 保持原版「速度倒序」主序（nodes 原序即速度序）
    order = {id(n): i for i, n in enumerate(nodes)}
    alive.sort(key=lambda n: order[id(n)])
    if MAX_JK_NODES > 0:
        alive = alive[:MAX_JK_NODES]
    return alive

# ---------------------------------------------------------------------------
# 前置节点: 拉取 CFnew 实例真实订阅（含 yxURL 优选）, 对齐原版解析/构建
# ---------------------------------------------------------------------------
def fetch_front_links():
    if not (CFNEW_BASE and CFNEW_UUID):
        log("未配置 CFNEW_BASE/CFNEW_UUID, 无前置 → 按原版语义退化为直连落地")
        return []
    for url in (f"{CFNEW_BASE}/{CFNEW_UUID}/sub", f"{CFNEW_BASE}/{CFNEW_UUID}"):
        try:
            r = requests.get(url, timeout=HTTP_TIMEOUT,
                             headers={"User-Agent": "v2rayNG/1.8 cfnew-yx-gate"})
            if r.status_code != 200 or not r.text.strip():
                continue
            text = r.text.strip()
            try:
                pad = text + "=" * (-len(text) % 4)
                decoded = base64.b64decode(pad).decode("utf-8", "replace")
                if "://" in decoded:
                    text = decoded
            except Exception:
                pass
            links = [ln.strip() for ln in text.splitlines()
                     if ln.strip().startswith(("vless://", "trojan://"))]
            if links:
                log(f"订阅获取成功: {len(links)} 条前置候选")
                return links
        except Exception as exc:
            log(f"订阅拉取失败({url.rsplit('/',1)[-1]}): {type(exc).__name__}")
    log("订阅不可用, 无前置 → 退化为直连落地")
    return []

def parse_link(link):
    """对齐原版 解析值链接: 只认 vless/trojan。"""
    try:
        from urllib.parse import urlsplit, parse_qs, unquote
        u = urlsplit(link)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        host = u.hostname or ""
        if host.startswith("[") and host.endswith("]"):
            host = host[1:-1]
        common = {
            "name": unquote(u.fragment) or f"{host}:{u.port or 443}",
            "server": host,
            "port": u.port or 443,
            "network": q.get("type", "ws"),
            "path": q.get("path", "/?ed=2048"),
            "host": q.get("host") or host,
            "sni": q.get("sni") or q.get("host") or host,
            "alpn": [a.strip() for a in q.get("alpn", "").split(",") if a.strip()],
            "fp": q.get("fp", "chrome"),
        }
        if link.startswith("vless://"):
            return {**common, "proto": "vless", "uuid": unquote(u.username or ""),
                    "tls": q.get("security") in ("tls", "reality")}
        if link.startswith("trojan://"):
            return {**common, "proto": "trojan",
                    "password": unquote(u.username or ""), "tls": True}
    except Exception:
        pass
    return None

def build_front_yaml(p):
    """对齐原版 构建值节点行（块级 YAML）。"""
    lines = [f"  - name: {yq(p['name'])}",
             f"    type: {p['proto']}",
             f"    server: {yq(p['server'])}",
             f"    port: {p['port']}"]
    if p["proto"] == "vless":
        lines += [f"    uuid: {p['uuid']}", "    udp: true",
                  f"    tls: {'true' if p['tls'] else 'false'}",
                  f"    client-fingerprint: {yq(p.get('fp') or 'chrome')}"]
    else:
        lines += [f"    password: {yq(p['password'])}", "    udp: true",
                  f"    client-fingerprint: {yq(p.get('fp') or 'chrome')}"]
    if p["tls"]:
        lines.append(f"    servername: {yq(p['sni'])}")
        if p["alpn"]:
            lines.append(f"    alpn: [{', '.join(yq(a) for a in p['alpn'])}]")
        lines.append("    skip-cert-verify: false")
    if p["network"] in ("ws", "xhttp"):
        lines += ["    network: ws", "    ws-opts:",
                  f"      path: {yq(p['path'])}",
                  "      headers:", f"        Host: {yq(p['host'])}"]
    elif p["network"] == "grpc":
        lines += ["    network: grpc", "    grpc-opts:",
                  f"      grpc-service-name: {yq(p['path'])}"]
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Clash YAML 生成（结构逐段对齐原版 生成家宽链式值）
# ---------------------------------------------------------------------------
def _indent_cert(text, pad):
    return "\n".join(pad + ln for ln in text.splitlines())

def build_clash_yaml(front, jk_nodes, cert, dns="https://223.5.5.5/dns-query"):
    # 原版: 有 TLS 前置就只用 TLS（落地隧道握手特征明显, 明文等于裸奔）
    tls_front = [p for p in front if p["tls"]]
    fronts = tls_front if tls_front else front
    front_names = [p["name"] for p in fronts]

    counters = {}
    items = []
    for n in jk_nodes:
        counters[n["country"]] = counters.get(n["country"], 0) + 1
        items.append({**n, "name": f"🏠 {n['country']}-家宽-{counters[n['country']]:02d}"})

    head = [
        "# cfnew 家宽订阅：CF 节点带路，落地是住宅宽带",
        "# 内核要 1.19.25 以上，老内核不认这类节点",
        "# 节点是网友共享的，掉线很正常，家宽自动会自己往下换",
        f"# 由 cfnew-yx-gate 预验证构建: 本批 {len(items)} 个节点发布前已通过 TCP 存活检测",
        "mixed-port: 7890", "allow-lan: false", "mode: rule", "log-level: info",
        "ipv6: false", "unified-delay: true", "tcp-concurrent: true",
        "external-controller: 127.0.0.1:9090",
        "dns:", "  enable: true", "  ipv6: false", "  enhanced-mode: fake-ip",
        "  fake-ip-range: 198.18.0.1/16", "  nameserver:",
        f"    - {dns}", "    - https://1.1.1.1/dns-query", "",
    ]
    proxies = ["proxies:"]
    for p in fronts:
        proxies.append(build_front_yaml(p))
    for i, n in enumerate(items):
        rows = [f"  - name: {yq(n['name'])}",
                "    type: openvpn",
                f"    server: {n['host']}",
                f"    port: {n['port']}",
                "    proto: tcp", "    username: vpn", "    password: vpn",
                f"    cipher: {n['cipher']}", f"    auth: {n['auth']}",
                "    udp: false", "    handshake-timeout: 30",
                "    remote-dns-resolve: true", "    dns: [ 8.8.8.8, 1.1.1.1 ]"]
        if front_names:
            rows.append(f'    dialer-proxy: "{GROUP_FRONT}"')
        if i == 0:  # 原版: 证书全站同一份, 第一个节点定锚点
            rows += ["    ca: &jkca |-", _indent_cert(cert["ca"], "      "),
                     "    cert: &jkcert |-", _indent_cert(cert["cert"], "      "),
                     "    key: &jkkey |-", _indent_cert(cert["key"], "      ")]
        else:
            rows += ["    ca: *jkca", "    cert: *jkcert", "    key: *jkkey"]
        proxies.append("\n".join(rows))

    def listed(names):
        return "\n".join(f'      - "{x}"' for x in names)

    by_speed = [n["name"] for n in items]                      # 原版: 自动组按速度排
    by_country = [n["name"] for n in
                  sorted(items, key=lambda x: x["country"])]   # 原版: 手选组按国家排
    groups = ["proxy-groups:"]
    if front_names:
        groups += [f'  - name: "{GROUP_FRONT}"', "    type: url-test",
                   "    url: https://www.gstatic.com/generate_204",
                   "    interval: 300", "    tolerance: 50",
                   "    proxies:", listed(front_names)]
    groups += [f'  - name: "{GROUP_AUTO}"', "    type: fallback",
               "    url: https://www.gstatic.com/generate_204",
               "    interval: 1800", "    lazy: true",
               "    proxies:", listed(by_speed)]
    groups += [f'  - name: "{GROUP_PICK}"', "    type: select",
               "    proxies:", listed(by_country)]
    main = [f'      - "{GROUP_AUTO}"', f'      - "{GROUP_PICK}"']
    if front_names:
        main.append(f'      - "{GROUP_FRONT}"')
    main.append("      - DIRECT")
    groups += [f'  - name: "{GROUP_MAIN}"', "    type: select",
               "    proxies:", "\n".join(main)]
    rules = ["rules:", "  - GEOIP,LAN,DIRECT,no-resolve",
             "  - GEOIP,CN,DIRECT,no-resolve", f"  - MATCH,{GROUP_MAIN}"]
    return "\n".join(head + proxies + [""] + groups + [""] + rules + [""])

# ---------------------------------------------------------------------------
# 发布（不可猜测 token 路径 + 失败回取上一版, 对齐原版 503 语义）
# ---------------------------------------------------------------------------
def jk_token():
    if JK_TOKEN:
        return JK_TOKEN
    seed = f"{CFNEW_BASE}|{CFNEW_UUID}|cfnew-yx-gate"
    return hashlib.sha256(seed.encode()).hexdigest()[:16]

def pages_base():
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if "/" in repo:
        owner, name = repo.split("/", 1)
        return f"https://{owner}.github.io/{name}"
    return ""

def fetch_previous(token):
    base = pages_base()
    if not base:
        return None
    try:
        r = requests.get(f"{base}/jk/{token}/clash.yaml", timeout=HTTP_TIMEOUT)
        if r.status_code == 200 and "proxies:" in r.text:
            return r.text
    except Exception:
        pass
    return None

def run(public_dir):
    """pipeline.py 调用入口。返回统计 dict（或 None 表示未启用）。"""
    if not JK_ENABLE:
        log("JK_ENABLE 未开启, 跳过家宽模块（CFnew 原生 ?target=vg 不受影响）")
        return None
    token = jk_token()
    out_dir = os.path.join(public_dir, "jk", token)
    stats = {"enabled": True}
    try:
        nodes, cert = fetch_vg_nodes()
        log(f"清单解析: {len(nodes)} 个住宅候选（已剔机房/非TCP）")
        alive = alive_filter(nodes)
        log(f"存活预验证: {len(alive)}/{len(nodes)} 通过 TCP 检测")
        if not alive:
            raise RuntimeError("本轮没有任何家宽节点存活")
        front = [p for p in (parse_link(l) for l in fetch_front_links()) if p]
        yaml_text = build_clash_yaml(front, alive, cert)
        os.makedirs(out_dir, exist_ok=True)
        with open(os.path.join(out_dir, "clash.yaml"), "w", encoding="utf-8") as f:
            f.write(yaml_text)
        stats.update({"vg_candidates": len(nodes), "vg_alive": len(alive),
                      "front": len(front),
                      "countries": len({n['country'] for n in alive})})
        log(f"clash.yaml 已生成: 家宽 {len(alive)} 个 / 前置 {len(front)} 个 "
            f"(路径: jk/{token[:4]}****/clash.yaml)")
    except Exception as exc:
        log(f"构建失败: {exc}")
        prev = fetch_previous(token)
        if prev:
            os.makedirs(out_dir, exist_ok=True)
            with open(os.path.join(out_dir, "clash.yaml"), "w", encoding="utf-8") as f:
                f.write(prev)
            log("已回取 Pages 上一版 clash.yaml 重新发布（对齐原版 503 沿用旧配置语义）")
            stats["reused_previous"] = True
        else:
            log("无上一版可回取, 本轮不发布家宽文件")
            stats["failed"] = True
    return stats

if __name__ == "__main__":
    run(os.path.join(os.path.dirname(os.path.abspath(__file__)), "public"))
```


### 文件: `requirements.txt`

```text
requests>=2.31.0
```


### 文件: `.github/workflows/update.yml`

```yaml
name: CFnew YX Pipeline

on:
  schedule:
    - cron: '7,37 * * * *'   # 每 30 分钟（错开整点高峰，减少 GitHub 排队延迟）
  workflow_dispatch:

permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: cfnew-yx-pipeline
  cancel-in-progress: false

jobs:
  build-and-publish:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run pipeline (collect + verify + build)
        run: python pipeline.py
        env:
          # ====== 以下全部可选，不配置就用内置默认值 ======
          # 候选 IP 来源（逗号分隔）
          # IP_SOURCES: "https://cf.090227.xyz/ct?ips=16&port=443"
          # 静态优选域名池（逗号分隔）
          # EDGE_DOMAINS: "www.visa.com.sg,www.wto.org"
          MAX_NODES: "60"
          CHECK_CONCURRENCY: "32"
          # ====== 可选：直接推送到你的 CFnew 实例（需面板开启 ae=yes）======
          CFNEW_BASE: ${{ secrets.CFNEW_BASE }}   # 如 https://xxx.pages.dev
          CFNEW_UUID: ${{ secrets.CFNEW_UUID }}   # UUID 或自定义路径(d变量)
          CFNEW_PUSH_MODE: "replace"
          # ====== 可选：CLASH 家宽订阅增强构建（保留原版全部功能 + 存活预验证）======
          JK_ENABLE: ${{ vars.JK_ENABLE }}        # 仓库 Variables 里设 yes 开启
          JK_TOKEN: ${{ secrets.JK_TOKEN }}       # 可选, 自定义订阅路径 token（不设则由 BASE+UUID 派生）

      - name: Ensure GitHub Pages is enabled (one-time auto setup)
        run: |
          code=$(curl -s -o /dev/null -w '%{http_code}' \
            -H "Authorization: Bearer $GITHUB_TOKEN" \
            "https://api.github.com/repos/$GITHUB_REPOSITORY/pages")
          echo "Pages API status: $code"
          if [ "$code" = "404" ]; then
            echo "首次运行: 自动启用 GitHub Pages (source = GitHub Actions)"
            curl -sS -X POST \
              -H "Authorization: Bearer $GITHUB_TOKEN" \
              -H "Content-Type: application/json" \
              -d '{"build_type":"workflow"}' \
              "https://api.github.com/repos/$GITHUB_REPOSITORY/pages" || true
            sleep 10
          fi
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}

      - name: Configure Pages
        uses: actions/configure-pages@v5

      - name: Upload Pages artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: 'public'

      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@v4

      - name: Show result
        run: |
          echo "仪表盘: ${{ steps.deployment.outputs.page_url }}"
          echo "yxURL : ${{ steps.deployment.outputs.page_url }}nodes.txt"
```


### 文件: `web/index.html`

```html
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>cfnew-yx-gate · CFnew 优选IP流水线</title>
<style>
  :root { --bg:#0a0e14; --panel:#101722; --line:#1d2a3a; --cyan:#00f0ff; --dim:#5c7a99; --txt:#c9e3f5; --ok:#2ee6a8; --warn:#ffb454; }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--txt); font-family: "Courier New", Consolas, monospace; padding: 24px 16px 60px; }
  .wrap { max-width: 980px; margin: 0 auto; }
  h1 { color: var(--cyan); font-size: 22px; letter-spacing: 2px; margin-bottom: 4px; }
  .sub { color: var(--dim); font-size: 12px; margin-bottom: 20px; }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px,1fr)); gap: 10px; margin-bottom: 20px; }
  .card { background: var(--panel); border: 1px solid var(--line); padding: 14px; }
  .card .v { color: var(--cyan); font-size: 26px; font-weight: bold; }
  .card .k { color: var(--dim); font-size: 11px; margin-top: 4px; }
  .urlbox { background: var(--panel); border: 1px solid var(--cyan); padding: 14px; margin-bottom: 20px; display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
  .urlbox code { color: var(--ok); word-break: break-all; flex: 1; font-size: 13px; }
  .urlbox button { background: transparent; border: 1px solid var(--cyan); color: var(--cyan); padding: 6px 14px; cursor: pointer; font-family: inherit; }
  .urlbox button:hover { background: var(--cyan); color: #000; }
  .hint { color: var(--dim); font-size: 12px; width: 100%; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { color: var(--cyan); text-align: left; border-bottom: 1px solid var(--cyan); padding: 8px 6px; font-size: 12px; }
  td { border-bottom: 1px solid var(--line); padding: 7px 6px; }
  td.lat { color: var(--ok); }
  .tag { color: var(--warn); }
  .note { color: var(--dim); font-size: 11px; margin-top: 16px; line-height: 1.7; }
  h2 { color: var(--cyan); font-size: 15px; margin: 24px 0 10px; }
</style>
</head>
<body>
<div class="wrap">
  <h1>▍CFNEW-YX-GATE</h1>
  <div class="sub" id="gen">—</div>

  <div class="urlbox">
    <code id="suburl">—</code>
    <button onclick="copyUrl()">复制</button>
    <div class="hint">把这个地址填进 CFnew 面板「优选IP来源URL (yxURL)」，保存一次，之后节点自动更新。</div>
  </div>

  <div class="cards" id="cards"></div>

  <h2>// 优选 IP 节点</h2>
  <table>
    <thead><tr><th>#</th><th>地址</th><th>名称</th><th>机房</th><th>来源</th><th>RTT</th></tr></thead>
    <tbody id="ipbody"></tbody>
  </table>

  <h2>// 优选域名</h2>
  <table>
    <thead><tr><th>#</th><th>域名</th><th>机房</th><th>RTT</th></tr></thead>
    <tbody id="dombody"></tbody>
  </table>

  <div class="note" id="note"></div>
</div>

<script>
const DATA = __DATA_JSON__;

const subUrl = location.origin + location.pathname.replace(/index\.html$/, '') + 'nodes.txt';
document.getElementById('suburl').textContent = subUrl;
document.getElementById('gen').textContent = '生成时间: ' + DATA.generated_at + ' · 每 30 分钟自动更新';

function copyUrl() {
  navigator.clipboard.writeText(subUrl).then(() => alert('已复制: ' + subUrl));
}

const s = DATA.stats || {};
const cardDefs = [
  [s.ip_published, '发布 IP 节点'],
  [s.domain_ok, '优选域名'],
  [s.colos, '覆盖机房'],
  [s.candidates, '候选总数'],
  [s.checked, '已验证'],
  [(s.duration_s || 0) + 's', '流水线耗时'],
];
document.getElementById('cards').innerHTML = cardDefs.map(
  ([v, k]) => `<div class="card"><div class="v">${v ?? '—'}</div><div class="k">${k}</div></div>`).join('');

document.getElementById('ipbody').innerHTML = (DATA.ip_nodes || []).map((n, i) =>
  `<tr><td>${i + 1}</td><td>${n.ip}:${n.port}</td><td>${n.name}</td><td>${n.colo}</td>` +
  `<td class="tag">${n.tag}</td><td class="lat">${n.latency_ms}ms</td></tr>`).join('') ||
  '<tr><td colspan="6">本轮无可用 IP 节点</td></tr>';

document.getElementById('dombody').innerHTML = (DATA.domain_nodes || []).map((n, i) =>
  `<tr><td>${i + 1}</td><td>${n.ip}:${n.port}</td><td>${n.colo}</td><td class="lat">${n.latency_ms}ms</td></tr>`).join('') ||
  '<tr><td colspan="4">本轮无可用域名</td></tr>';

document.getElementById('note').textContent = '说明: ' + (s.note || '') +
  '。数据源: 公开优选 API + Cloudflare 官方网段采样; 验证方式: TCP + TLS + /cdn-cgi/trace 三段验证 (GitHub Runner 直测, 无需额外 Worker)。';
</script>
</body>
</html>
```


### 文件: `README.md`

`````markdown
# cfnew-yx-gate — CFnew 优选IP全自动流水线 🚀

自动从多个公开优选 API 抓取 Cloudflare 入口 IP，在 **GitHub Runner 上直接三段验证**
（TCP 连通 → TLS 握手 → `/cdn-cgi/trace` 回显），按机房标注国旗/中文地名、按延迟排序，
发布为 [byJoey/cfnew](https://github.com/byJoey/cfnew) 的 `yxURL` 直接可用的节点清单。
**每 30 分钟自动更新一次。**

> 核心价值：优选 IP 会随时间失效，手动测速、手动往 CFnew 面板粘贴太痛苦。
> 本仓库把它全自动化了——你只需把 `nodes.txt` 的网址填进 CFnew 面板
> 「优选IP来源URL (yxURL)」一次，之后节点每 30 分钟自动换，零手动。

**与 [heleihub/gate](https://github.com/heleihub/gate) 的思路一脉相承，但有两点不同：**

1. gate 服务的是 edgetunnel（`$sstp://` 链式语法）；本项目输出的是 **CFnew 的
   `yxURL` 行格式**（`IP:端口#名称`，逐行解析，IPv6 自动加方括号，缺省端口 443），
   并额外支持 CFnew 的 `preferred-ips` API 直推。
2. gate 的节点验证依赖一个额外部署的检测 Worker；本项目把验证搬到 GitHub Runner
   本机完成，**不需要部署任何额外 Worker —— Cloudflare 免费计划零资源占用**。

---

## 架构（数据流向）

```
公开优选 API (电信/联通/移动/BestCF…)      Cloudflare 官方网段
        │ (每 30 分钟, GitHub Actions 抓取)   │ (候选不足时随机采样兜底)
        ▼                                     ▼
            合并候选 IP ──── 去重 ──── + 静态优选域名池
                              │
                              ▼
         GitHub Runner 三段验证 (无需检测 Worker)
         TCP 连通 → TLS 握手(SNI) → GET /cdn-cgi/trace
                              │  提取 colo 机房码 + RTT
                              ▼
       按延迟排序 → 国旗/中文地名命名 → 截取 TOP 60
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
   public/nodes.txt (GitHub Pages)   (可选) CFnew preferred-ips API 直推
              │
              ▼
   CFnew 面板「优选IP来源URL (yxURL)」填 https://…/nodes.txt
              │  CFnew 每次生成订阅时自动 fetch → 逐行解析成优选节点
              ▼
   客户端刷新订阅 → 优选 IP 每 30 分钟自动换，零手动
```

---

## 一、完整部署教程（从零开始）

### 前置条件

- 一个 GitHub 账号
- 一个已部署好的 [byJoey/cfnew](https://github.com/byJoey/cfnew)（Pages 或 Worker 均可，
  Cloudflare **免费计划即可**；还没部署的看
  [CFnew 自动更新部署教程](https://joeyblog.net/cfnew-auto-update.html)）

### 第 1 步：Fork / 导入本仓库

点 **Fork**（记得把仓库名改成你自己的，别用默认名），或者下载源码自己 push 一份新仓库。

### 第 2 步：启用 Actions（有个坑）

Fork 过来的仓库 GitHub 默认不启用 Actions：

1. 仓库顶部 **Actions** 标签 → 点启用 workflow
2. 本项目 workflow 只需要默认的只读内容权限 + Pages 写权限（已在 yml 里声明），
   **不需要**像 cfnewup 那样手动改 Workflow permissions

### 第 3 步：手动跑一次

Actions → **CFnew YX Pipeline** → Run workflow。
首次运行会自动启用 GitHub Pages（source = GitHub Actions），约 1~2 分钟跑完。

### 第 4 步：确认产物

```
仪表盘:  https://你的GitHub用户名.github.io/仓库名/
节点清单: https://你的GitHub用户名.github.io/仓库名/nodes.txt
```

`nodes.txt` 里是一堆 `104.xx.xx.xx:443#🇯🇵东京-NRT-电信-01` 的行，就说明打通了。

### 第 5 步：填进 CFnew（只需一次）

1. 打开你的 CFnew 面板：`https://你的域名/{UUID}`（或自定义路径）
2. 找到 **「优选IP来源URL (yxURL)」** 输入框，粘贴上面的 `nodes.txt` 地址
3. 确认 **「启用自定义优选」(egi)** 处于开启状态（默认开启）
4. 保存（右下角「保存全部」或 Ctrl+S）
5. 客户端刷新订阅 → 每次刷新 CFnew 都重新拉取一次 `nodes.txt`，节点自动更新

> 也可以不用面板，直接给 Pages/Worker 加环境变量 `yxURL` = 上述地址。

---

## 二、（可选）API 直推模式

除了 yxURL 拉取，还可以让流水线每轮主动把 TOP 30 优选 IP 写入你的 CFnew KV：

1. CFnew 面板里开启 **「允许API管理」**（`ae=yes`）并保存
2. 本仓库 Settings → Secrets and variables → Actions，添加两个 Secret：
   - `CFNEW_BASE`：你的 CFnew 地址，如 `https://xxx.pages.dev`
   - `CFNEW_UUID`：你的 UUID（设置过 `d` 自定义路径就填路径）
3. 下一轮运行时自动生效（默认 replace 模式：先清空再写入）

> yxURL 模式与 API 模式二选一即可，同时开也不冲突（名称会重复显示两套）。
> 推荐 yxURL：不往 KV 写数据、不需要开 API 权限，攻击面更小。

---

## 二点五、（可选）CLASH 家宽订阅增强构建

**原版 CFnew 的家宽链式（`?target=vg`）功能本项目完全不触碰、不影响**——而且 yxURL
喂进去的优选节点会自动成为其「⚡ CF前置」组成员，等于白赚一层优化。

在此之上，本仓库提供可选的 `jk_builder` 模块：**1:1 保留原版 CLASH 家宽的全部功能**
（同样的节点源与机房剔除规则、速度倒序、🏠 国家-家宽-NN 命名、证书锚点、
⚡CF前置/🏠家宽自动/🏠家宽节点/🚀节点选择四分组、TLS-only 前置、无前置退化直连、
fake-ip DNS 与 GEOIP 规则、拉取失败沿用上一版），并增强一点：
**发布前在 GitHub Runner 对每个家宽节点做 TCP 存活预验证，死节点不进订阅**
（原版实测约一半连不上，预验证后 fallback 收敛更快）。

开启方法：

1. 仓库 Settings → Secrets and variables → Actions：
   - **Variables** 加 `JK_ENABLE` = `yes`
   - **Secrets** 加 `CFNEW_BASE`、`CFNEW_UUID`（用于拉取你的订阅作前置；
     可选 `JK_TOKEN` 自定义订阅路径 token）
2. 跑一次 workflow，订阅地址为：
   ```
   https://<用户名>.github.io/<仓库名>/jk/<token>/clash.yaml
   ```
   token 默认由 BASE+UUID 派生，本地计算：
   ```bash
   python3 -c "import hashlib;print(hashlib.sha256('https://你的域名|你的UUID|cfnew-yx-gate'.encode()).hexdigest()[:16])"
   ```

> ⚠️ 隐私：clash.yaml 含你的 UUID。GitHub Pages 免费版仓库是公开的，靠不可猜测
> token 路径保护（日志里也不会打印完整 token），请勿把地址外传。
> 介意的话就只用原版 `?target=vg`（功能一样，少了预验证而已）。
> 内核要求与原版相同：mihomo 1.19.25+（Clash Verge Rev / FlClash / CMFA）。

---

## 三、配置速查表（环境变量，全部可选）

| 变量 | 默认 | 说明 |
| :-- | :-- | :-- |
| `IP_SOURCES` | 内置 4 个公开 API | 候选 IP 来源，逗号分隔 |
| `EDGE_DOMAINS` | 内置 7 个常用优选域名 | 静态优选域名池，逗号分隔 |
| `MAX_NODES` | 60 | nodes.txt 中 IP 节点上限 |
| `CHECK_CONCURRENCY` | 32 | 验证并发数 |
| `MIN_CANDIDATES` / `RANDOM_SAMPLE` | 12 / 32 | 候选不足时的官方网段随机采样兜底 |
| `TRACE_HOST` | www.cloudflare.com | 验证用的 SNI/Host |
| `CFNEW_BASE` / `CFNEW_UUID` | 空 | 配置后启用 API 直推 / 家宽前置拉取 |
| `CFNEW_PUSH_MODE` | replace | replace=先清空再写入, append=追加 |
| `JK_ENABLE` | 空 | `yes` 开启 CLASH 家宽增强构建 |
| `JK_TOKEN` | 由 BASE+UUID 派生 | 家宽订阅路径 token |
| `JK_MAX_NODES` | 0（全量，同原版） | 家宽节点数上限 |
| `JK_ALIVE_TIMEOUT` / `JK_ALIVE_CONCURRENCY` | 6 / 32 | 存活预验证参数 |

改默认值：编辑 `.github/workflows/update.yml` 的 `env:` 段即可。

---

## 四、常见问题

### 节点数很少 / 全是「随机」来源
公开优选 API 可能被限流或失效。把 `IP_SOURCES` 换成你自己信任的来源，
或用 [byJoey/yx-tools](https://github.com/byJoey/yx-tools/releases) 本地测一批填进去。

### 延迟数字和我本地实测对不上
正常。流水线显示的 RTT 是 **GitHub Runner（海外）测得**，只代表「节点存活、
确实是 CF 边缘」，不代表你本地的优选质量。候选来源本身（电信/联通/移动 API）
已经是国内视角的优选结果，流水线做的是**存活二次过滤**。

### 30 分钟没更新
GitHub 定时任务高峰期会延迟几分钟到十几分钟，属正常现象；
也可以随时 Actions 里手动触发。注意：**仓库 60 天无 commit，GitHub 会自动暂停
定时任务**，届时去 Actions 页点一下 enable 即可恢复。

### CFnew 面板里看不到这些节点
检查三件事：① yxURL 填的是 `nodes.txt` 完整地址且能浏览器直接打开；
② 「启用自定义优选」(egi) 开着；③ 保存后重新拉取了订阅（面板上改完要点保存）。

### 和 cfnewup（官方自动更新仓库）冲突吗？
不冲突，职责不同：cfnewup 负责**升级 CFnew 程序本身**，本项目负责**喂新鲜的优选 IP**。
两个一起用 = 程序和数据都全自动。

---

## 引用与致谢

| 项目 | 用途 |
| :-- | :-- |
| [byJoey/cfnew](https://github.com/byJoey/cfnew) | 节点最终通过它使用（yxURL / preferred-ips API） |
| [heleihub/gate](https://github.com/heleihub/gate) | 流水线架构思路来源 |
| [CFnew 自动更新部署教程](https://joeyblog.net/cfnew-auto-update.html) | Pages + Actions 自动化部署方法论 |
| cf.090227.xyz / ipdb.api.030101.xyz | 公开优选 IP 数据源 |

> 仅供学习与技术研究，请遵守当地法律法规。
`````


---

## 4. 部署步骤（逐条执行）

### 4.1 创建仓库并推送

```bash
REPO_NAME="cfnew-yx-gate-$(date +%s | tail -c 5)"   # 随机后缀避免同名

cd cfnew-yx-gate
git init -b main
git add -A
git commit -m "init: cfnew-yx-gate pipeline"
gh repo create "$REPO_NAME" --public --source=. --push
USER_LOGIN=$(gh api user -q .login)
```

> 无 gh CLI 时：`curl -X POST -H "Authorization: Bearer $GITHUB_TOKEN" https://api.github.com/user/repos -d '{"name":"'$REPO_NAME'"}'`，再 `git remote add origin … && git push -u origin main`

### 4.2 触发首次运行（会自动启用 GitHub Pages）

```bash
gh workflow run "CFnew YX Pipeline" --repo "$USER_LOGIN/$REPO_NAME"
sleep 30
RUN_ID=$(gh run list --repo "$USER_LOGIN/$REPO_NAME" --limit 1 --json databaseId -q '.[0].databaseId')
gh run watch --repo "$USER_LOGIN/$REPO_NAME" "$RUN_ID"
```

> Fork 而非新建仓库时，必须先去 Actions 标签页手动点「启用 workflow」。
> workflow 首次运行自动启用 Pages（build_type=workflow）；若被组织策略拦截，
> 手动 Settings → Pages → Source 选 **GitHub Actions** 后重跑。

### 4.3 验证产物

```bash
curl -fsS "https://$USER_LOGIN.github.io/$REPO_NAME/nodes.txt" | head -5
# 期望: 104.17.28.3:443#🇺🇸西雅图-SEA-BestCF-01 这样的行
# 判据: HTTP 200 且每行匹配 ^[^:#]+:\d+#.+$
```

### 4.4 CFnew 面板填 yxURL（推荐，人工/浏览器）

1. 打开 `https://<CFnew域名>/<UUID>`（或自定义路径）
2. 「优选IP来源URL (yxURL)」填 `https://<user>.github.io/<repo>/nodes.txt`
3. 确认「启用自定义优选」(egi) 开启（默认开启）→ 保存（Ctrl+S）
4. 客户端刷新订阅：应出现「🇺🇸西雅图-SEA-…」等节点名

非面板替代：给 CFnew 加环境变量 `yxURL`=上述地址后重新部署。

### 4.5 （可选）API 直推模式

```bash
# 前提: CFnew 面板已开启「允许API管理」(ae=yes)
gh secret set CFNEW_BASE --repo "$USER_LOGIN/$REPO_NAME" --body "https://xxx.pages.dev"
gh secret set CFNEW_UUID --repo "$USER_LOGIN/$REPO_NAME" --body "<UUID或自定义路径>"
```

### 4.6 （可选）开启 CLASH 家宽增强订阅

```bash
# 1) 需要 4.5 的两个 Secret（用于拉取你的订阅作「⚡ CF前置」）
# 2) 开启模块开关（Repository Variable, 不是 Secret）：
gh variable set JK_ENABLE --repo "$USER_LOGIN/$REPO_NAME" --body "yes"
# 3) （可选）自定义订阅路径 token：
# gh secret set JK_TOKEN --repo "$USER_LOGIN/$REPO_NAME" --body "<随机串>"

# 4) 重跑一次 workflow 后，计算订阅地址（token 由 BASE|UUID 派生）：
TOKEN=$(python3 -c "import hashlib;print(hashlib.sha256('<CFNEW_BASE>|<CFNEW_UUID>|cfnew-yx-gate'.encode()).hexdigest()[:16])")
echo "家宽订阅: https://$USER_LOGIN.github.io/$REPO_NAME/jk/$TOKEN/clash.yaml"
```

家宽订阅与原版 `?target=vg` 功能一致（四分组/证书锚点/规则全保留），
额外做了存活预验证。**客户端内核要求 mihomo 1.19.25+**（Clash Verge Rev / FlClash /
Clash Meta for Android；Stash/Surge/sing-box 不支持 openvpn 节点类型）。

> ⚠️ 隐私：clash.yaml 含 UUID，靠不可猜测 token 路径保护，请勿外传地址；
> 介意者改用 CFnew 原生 `https://<域名>/<UUID>/sub?target=vg`（需面板勾「开启家宽链式」）。

---

## 5. 验收清单（Agent 自检）

- [ ] `gh run list` 最近一次 run 结论为 `success`
- [ ] 仪表盘 `https://<user>.github.io/<repo>/` 可打开且有统计数字
- [ ] `nodes.txt` 非空且行格式匹配 `^[^:#]+:\d+#.+$`
- [ ] （yxURL 模式）CFnew 订阅出现带国旗/机房码的节点
- [ ] （API 模式）workflow 日志「API 推送」段 HTTP 200
- [ ] （家宽模式）`jk/<token>/clash.yaml` 返回 200，`yaml.safe_load` 通过，
      含 `type: openvpn`、`dialer-proxy: "⚡ CF前置"`、四个 proxy-groups

---

## 6. 故障排查速查

| 症状 | 原因 | 处理 |
| :-- | :-- | :-- |
| 首次 run 报 Pages 404/403 | Pages 自动启用被策略拦截 | Settings → Pages → Source=GitHub Actions，重跑 |
| nodes.txt 全是「随机」来源 | 公开优选 API 被限流 | 改 `IP_SOURCES` 换源 |
| CFnew 不显示节点 | yxURL 填错 / egi 关闭 / 没保存 | 按 4.4 逐项核对 |
| API 推送非 200 | 未开 `ae=yes` 或 UUID/路径错 | 面板开「允许API管理」 |
| 家宽模块日志「无前置 → 退化直连」 | CFNEW_BASE/UUID 未配或订阅拉取失败 | 核对 4.5 两个 Secret |
| 家宽 clash.yaml 404 | JK_ENABLE 没设 / token 算错 | 核对 4.6；token 种子格式 `BASE|UUID|cfnew-yx-gate` |
| 家宽节点连不上 | 志愿者节点掉线是常态（原版同） | fallback 组会自动换；刷新订阅拿新一批 |
| 定时任务停了 | 仓库 60 天无 commit 被暂停 | Actions 页 enable；或定期空 commit |

---

## 附录 A：安全与合规说明

- workflow 仅需 `contents: read` + `pages: write`，不写仓库、不需要 PAT
- 家宽订阅文件含 UUID，仅靠不可猜测路径保护（公共仓库 Pages 无法设访问控制），
  日志中不打印完整 token；高敏感场景请改用 CFnew 原生 `?target=vg`
- 仅供学习与技术研究，使用者需遵守当地法律法规

## 附录 B：CFnew 本体还没部署？

按官方教程先部署 CFnew（Cloudflare 免费计划即可）：
https://joeyblog.net/cfnew-auto-update.html
要点：环境变量 `u`=UUID；KV 绑定变量名 `C`；兼容性日期 `2026-01-20`；
KV 绑定后要手动「重试部署」一次才生效；家宽链式需面板勾「开启家宽链式」或 `jk=yes`。
