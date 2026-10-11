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
