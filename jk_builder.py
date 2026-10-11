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
