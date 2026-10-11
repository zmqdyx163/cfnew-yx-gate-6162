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

## 当前部署实例（2026-10-12 实际运行状态）

| 项目 | 值 |
| :-- | :-- |
| GitHub 仓库 | `zmqdyx163/cfnew-yx-gate-6162` |
| 仪表盘 | https://yx.my2d.eu.cc/ （自定义域名，DNS-only CNAME → GitHub Pages） |
| 节点清单 `yxURL` | https://yx.my2d.eu.cc/nodes.txt （已填入 CFnew 面板） |
| CFnew 面板 | `https://zmq.kdns.fr/{UUID}`（Pages 项目 `cfnew-auto`，KV 绑定 `c`） |
| 家宽模块 `jk_builder` | **已关闭**（仓库 Variable `JK_ENABLE=no`；曾开启验证跑通：64 候选 → 32 存活 → 家宽订阅 `jk/c6ab9c0310805c1e/clash.yaml`；重新开启改回 `yes` 即可） |
| 定时流水线 | 每 30 分钟（`:07/:37` 分触发），GitHub 高峰期可能延迟几分钟 |

**运维一键脚本**：`enable_workflow.bat`（双击）——仓库 60 天无 commit 会自动暂停定时任务，
脚本自动检查状态 → 恢复启用 → 触发一次运行验证结果。等价的 API 操作：
`PUT /repos/{owner}/{repo}/actions/workflows/update.yml/enable`。

**CFnew 面板侧必查项（yxURL 断供时按序排查）**：

1. 面板 `yx`（静态优选IP列表）必须**为空**——`yx` 非空会短路 `yxURL` 分支，两者互斥
2. 面板「GitHub优选」开关（配置键 `egi`）必须为**开启**，否则 yxURL 分支被跳过
3. 改完配置等约 1 分钟（isolate 30s 缓存 + KV 跨节点传播）再拉订阅

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
