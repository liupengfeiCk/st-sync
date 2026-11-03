# st-sync

st-sync 是给 [SillyTavern](https://github.com/SillyTavern/SillyTavern)（酒馆）用的增量同步代理，用来省流量，主要面向用手机远程访问酒馆的场景。它不修改酒馆本身，客户端也不需要安装任何东西，浏览器打开原来的地址就能用。

## 解决什么问题

酒馆的很多接口每次都收发整份文档：保存设置会上传完整的 `settings.json`，加载页面会下载全部角色卡和设置，保存聊天会上传整段聊天记录。插件多、聊天长的时候，这些文档动辄几十 MB，而每次真正变化的往往只有几 KB，所以跑一楼就可能消耗几百 MB 流量。

st-sync 把这些请求和响应按内容切成块，浏览器和服务器各自缓存见过的块，之后只传对方没有的块。对于变化很少的大文档，实际传输量通常只有原来的百分之几。

## 要求

- **系统：** Linux + systemd（部署脚本依赖它）。建议 systemd 254 或更高版本：在更老的版本上，st-sync 第一次崩溃就会切回酒馆直连，而不是先尝试重启几次。
- **软件：** Node.js 18.15 或更高版本，和酒馆共用同一个就行；另外需要 curl。st-sync 本身没有任何依赖。
- **酒馆：** 已在 SillyTavern 1.15.0 上验证。酒馆不能自己开 HTTPS（`config.yaml` 里 `ssl.enabled: false`）；需要 HTTPS 的话，放在 st-sync 前面的反向代理上（见“放在 nginx / Caddy 后面”）。
- **浏览器：** 需要支持 IndexedDB、Web Worker 和 ReadableStream，近几年的手机和电脑浏览器都可以。

## 快速开始

1. 停掉你现在启动酒馆的方式，比如正在运行的 `start.sh`、pm2 或者你自己写的 systemd unit。之后改由 st-sync 安装的 systemd unit 来启动酒馆。
2. 安装：

   ```bash
   git clone <仓库地址> st-sync && cd st-sync
   sudo ./deploy/install.sh --st-dir /path/to/SillyTavern
   ```

   脚本会把代码复制到 `/opt/st-sync`，生成 systemd unit、`/etc/st-sync/env` 和 `st-mode` 命令，并启动守护进程。它不会切换模式：安装完之后仍然是酒馆直接对外提供服务。
3. 切换到 st-sync，并设为开机默认：

   ```bash
   sudo st-mode proxy
   sudo st-mode boot proxy
   ```

4. 在浏览器里打开原来的地址并刷新页面。在开发者工具的控制台执行 `__stSync.info()`，看到 `active: true` 就说明已经生效。

默认情况下，st-sync 接管酒馆 `config.yaml` 里原本的端口，所以客户端的访问地址不用改；酒馆自己改为只在 `127.0.0.1` 上的另一个端口监听。

### `install.sh` 参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--st-dir` | 必填 | 酒馆目录，也就是 `server.js` 所在的目录 |
| `--port` | `config.yaml` 里的 `port`，没有则 8000 | 对外端口，由 st-sync 监听 |
| `--upstream-port` | 对外端口 + 10 | 酒馆在 127.0.0.1 上监听的端口 |
| `--user` | 酒馆目录的所有者 | 酒馆和 st-sync 用哪个用户运行 |
| `--listen-host` | `0.0.0.0` | st-sync 监听的地址；放在反向代理后面时改为 `127.0.0.1` |
| `--prefix` | `/opt/st-sync` | 代码安装位置 |

重复运行 `install.sh` 会更新代码和 unit 文件，`/etc/st-sync/env` 里你自己加的其他设置会保留。

## 工作原理

```text
浏览器 (shim.js + worker.js + IndexedDB)  ⇄  st-sync :对外端口  ⇄  SillyTavern 127.0.0.1:内部端口
                                                 │
                                                 └ 块库 /var/lib/st-sync
```

- **分块。** `cdc.js` 是纯 JS 实现，浏览器和服务器共用一份。它用 Gear 滚动哈希按内容切块，块大小在 2KB 到 64KB 之间；每个块再用 MurmurHash3 x86_128 算出 128 位哈希。因为切点只取决于内容，文档中间插入或删除内容时，只会影响附近一两个块。
- **注入。** st-sync 在酒馆返回的 HTML `<head>` 最前面插入 `/__sync/shim.js`，保证它先于酒馆和插件的所有脚本运行。shim 接管同源的 `fetch`（上传和下载）和 `XMLHttpRequest`（只接管上传）。
- **下载。** shim 带上 `X-Sync-Accept: cdc1`。响应超过 64KB 时，st-sync 不返回正文，而是返回清单（`SYD1`），列出各个块的哈希。shim 先从 IndexedDB 取本地已有的块，缺的块再通过 `POST /__sync/chunks` 批量获取，然后按 4MB 一个窗口拼成流交给调用方。调用方拿到的仍然是一个普通的 `Response`。
- **上传。** shim 把超过 64KB 的请求体切块后，打成 `SYC1` 信封（`X-Sync-Encoding: cdc1`）发出。信封里列出全部块的哈希，只附上服务器还没有的块的数据。st-sync 还原出原始请求体，再以流的方式转发给酒馆。如果某个块服务器其实没有，返回 `409` 并带上 `X-Sync-Missing`，shim 补上这些块后重发。大于 1MB 的内容在 Web Worker 里切块，不卡主线程。
- **XHR 上传。** XHR 没法在调用方不知情的情况下重发。所以只有在当前页面已经确认过块库 ID（`X-Sync-Store`）和用户（`X-Sync-Scope`）之后，XHR 上传才会依赖服务器上已有的块，否则把所有块都带上。如果 XHR 收到 `409` 或 `X-Sync-Error`，这次请求会失败，同时本地的“已确认”记录全部作废，因此只会失败这一次。
- **自动退出。** 只要某个响应没有带 `X-Sync-Proxy` 头，或者这个头的值是 `off`，shim 就停用自己，之后的请求都按原样发出。所以 st-sync 停掉或者被绕过时，页面照样能用。

## 安全

- **认证仍由酒馆负责。** st-sync 本身不做任何认证，所有请求都转给酒馆处理，basic auth、用户账号和白名单照常生效。补块接口 `/__sync/chunks` 也要先通过酒馆的认证，才会返回块。为了让这些认证生效，酒馆必须带 `--listen true` 启动，因为酒馆只在 listen 为 true 时才检查 basic auth 和白名单；生成的 unit 已经这样设置，同时只监听 `127.0.0.1`。
- **来源 IP 和白名单。** 酒馆开启 `enableForwardedWhitelist`（默认开启）时，白名单优先看 `X-Real-IP`，其次看 `X-Forwarded-For` 里的第一个地址。所以默认情况下，st-sync 会丢掉客户端自己带的这两个头，改成它实际看到的连接地址，防止有人伪造来源绕过白名单。请保持 `enableForwardedWhitelist: true`；否则酒馆看到的所有请求都来自 127.0.0.1，白名单等于失效。
- **用户隔离。** 每个酒馆用户的块单独存放，用户由酒馆的 `/api/users/me` 确定。浏览器的“已确认”记录绑定在块库 ID 和用户上，两者任何一个变了，记录就全部作废。
- **本地缓存。** 浏览器会把下载过的块（包括聊天内容）缓存在 IndexedDB 里，上限 192MB。在共用设备上，可以清除站点数据，或者设置 `localStorage['st-sync-disabled'] = '1'` 不使用 st-sync。

## 放在 nginx / Caddy 后面

如果你已经用反向代理提供 HTTPS，就把它的上游指向 st-sync，而不是酒馆：

1. 安装时加上 `--listen-host 127.0.0.1`，让 st-sync 只能通过反向代理访问。
2. 在 `/etc/st-sync/env` 里加一行 `ST_SYNC_TRUST_FORWARDED=1`，然后 `sudo systemctl restart st-sync`。这样 st-sync 会信任反向代理传来的 `X-Real-IP` 和 `X-Forwarded-For`。只有在 st-sync 无法被直接访问时才能这样设置。
3. 反向代理里按酒馆原来的要求配置：生成回复是流式的，nginx 需要 `proxy_buffering off`；`client_max_body_size` 要足够大；`X-Sync-*` 请求头和响应头要原样传递，nginx 默认就是这样。

## 资源上限与逃生

st-sync 的设计原则是：任何情况下都不能把服务器拖垮，出问题时酒馆要能自动恢复直连。

**内存：**
- 请求体和响应体都不会整个读进内存，而是按块（不超过 64KB）流式处理，块直接写进磁盘上的块库。
- 同时最多处理 3 个大请求，多出来的排队。上传最多等 30 秒，到时还没空位也照常处理；下载最多等 15 秒，到时直接按原样转发，内容不受影响。
- 实际在用的内存超过 64MB，或者常驻内存超过 200MB 时，新来的请求一律按原样转发。

**磁盘：**
- 剩余空间少于 3GB 时停止增量同步。
- 剩余空间少于 512MB 时，连上传也会拒绝（返回 `507`）。
- 块库上限 1GB，保留 30 天，但最近 8 天用过的块永远不会被清理。这个期限比浏览器端 7 天的确认有效期更长，所以浏览器认为服务器有的块，一定还在。

**systemd 限制（`st-sync.service`）：**
- `MemoryHigh=200M`、`MemoryMax=256M`、`MemorySwapMax=0`。
- `CPUQuota=80%`、`TasksMax=64`、`OOMScoreAdjust=800`、`Nice=5`。
- V8 堆上限 `--max-old-space-size=96`。

**逃生：**
- st-sync 崩溃后会自动重启。5 分钟内重启超过 5 次，systemd 就启动 `sillytavern-direct`，由酒馆自己接管对外端口。
- `st-sync-guard` 每 5 秒检查一次健康接口，以及能否通过 st-sync 访问到酒馆。连续 3 次失败（大约 30 秒）就切换到直连。
- `sudo st-mode off` 让 st-sync 只按原样转发，不再做增量同步；已经发出的分块上传仍然会被还原，避免请求半路失败。
- 浏览器端可以单独关闭：在控制台执行 `localStorage['st-sync-disabled'] = '1'`，然后刷新页面。

## 日常使用

| 命令 | 作用 |
|---|---|
| `st-mode status` | 查看各个服务的状态、开机设置和健康信息 |
| `st-mode proxy` / `st-mode direct` | 由 st-sync 提供服务 / 由酒馆直接提供服务 |
| `st-mode off` / `st-mode on` | st-sync 继续运行，但只按原样转发 / 恢复增量同步 |
| `st-mode boot proxy` / `st-mode boot direct` | 开机时启动哪一个 |

切换模式、重启 st-sync 都会中断几秒，最好不要在生成回复的过程中进行。

- **更新：** `git pull && sudo ./deploy/install.sh --st-dir /path/to/SillyTavern && sudo systemctl restart st-sync`。浏览器下次加载页面时会自动拿到新版 shim，URL 里带有版本号。
- **酒馆自己的输出：** `journalctl -u sillytavern`（直连模式下是 `-u sillytavern-direct`）。
- **健康接口：** `curl 127.0.0.1:<对外端口>/__sync/health`，返回版本号、当前在处理的大请求数（`heavy`）、排队数（`waiting`）、常驻内存、堆内存、堆外内存和块库大小。
- **访问日志：** `/var/log/st-sync/access.log`，每个请求一行 JSON，超过 50MB 自动轮转。主要字段：
  - `mode` 是处理方式：`up`/`down`/`sync` 表示增量同步，`pass` 表示按原样转发，`chunks` 表示补块请求。
  - `in`/`out` 是实际传输的字节数，`orig` 是还原后的原始大小。
  - `c` 是客户端标记：`.f` 表示经过 shim 的 `fetch`，`.x` 表示经过 shim 的 XHR；没有这个字段，说明请求没经过 shim。
- **统计某个客户端的流量：** `node /opt/st-sync/test/log-summary.js <客户端IP> [起始时间，ISO 格式]`
- **在浏览器里查看：** 控制台执行 `__stSync.info()`。

### 配置

改 `/etc/st-sync/env`，然后 `sudo systemctl restart st-sync`。端口和监听地址请通过重新运行 `install.sh` 修改，因为 unit 文件里也用到它们。

| 变量 | 默认值 |
|---|---|
| `ST_SYNC_LISTEN_HOST` / `ST_SYNC_LISTEN_PORT` | `0.0.0.0` / 8000 |
| `ST_SYNC_UPSTREAM_HOST` / `ST_SYNC_UPSTREAM_PORT` | `127.0.0.1` / 8010 |
| `ST_SYNC_TRUST_FORWARDED` | `0`；设为 `1` 时信任客户端传来的 `X-Real-IP` 和 `X-Forwarded-For` |
| `ST_SYNC_STORE_DIR` | `/var/lib/st-sync` |
| `ST_SYNC_LOG_FILE` | `/var/log/st-sync/access.log` |
| `ST_SYNC_OFF_FILE` | `/etc/st-sync/off` |
| `ST_SYNC_THRESHOLD` | 64KB，小于这个大小的内容不做增量处理 |
| `ST_SYNC_STORE_MAX_BYTES` / `ST_SYNC_RETENTION_DAYS` | 1GB / 30 天 |
| `ST_SYNC_MIN_FREE_DISK` | 3GB |
| `ST_SYNC_LIVE_SOFT_LIMIT` / `ST_SYNC_RSS_SOFT_LIMIT` | 64MB / 200MB |
| `ST_SYNC_MAX_HEAVY` | 3 |

### 不用 systemd 手动运行

`proxy.js` 本身不依赖 systemd，可以直接运行：

```bash
ST_SYNC_LISTEN_PORT=8000 ST_SYNC_UPSTREAM_PORT=8010 \
ST_SYNC_STORE_DIR=./store ST_SYNC_LOG_FILE=./access.log node --max-old-space-size=96 proxy.js
```

这时酒馆要用 `--port 8010 --listen true --listenAddressIPv4 127.0.0.1` 启动，并且开着 basic auth 或白名单。注意，这种方式没有资源上限、自动重启和守护进程。

## 卸载

```bash
sudo /opt/st-sync/deploy/uninstall.sh           # 保留块库和日志
sudo /opt/st-sync/deploy/uninstall.sh --purge   # 连同块库、日志、配置和代码一起删除
```

卸载后由 `sillytavern-direct.service` 直接运行酒馆，开机也会启动。如果想回到你原来的启动方式，执行 `sudo systemctl disable --now sillytavern-direct`，然后删除 `/etc/systemd/system/sillytavern-direct.service`。

## 测试

| 命令 | 内容 |
|---|---|
| `node test/cdc-test.js` | 分块和信封格式的单元测试 |
| `bash test/run-local.sh` | 用假的酒馆服务（`test/fake-upstream.js`）跑协议测试和负载测试，使用 18345 和 18355 端口。macOS 上常驻内存的数字不可信，可以加 `LIMIT_MB=999` 跳过这项检查 |
| `test/page.html` | shim 的浏览器端测试：按 `run-local.sh` 里的方式启动那两个服务，然后用浏览器打开 `http://127.0.0.1:18345/` |
| `bash test/run-server.sh` | 在 Linux 服务器上跑同样的测试，每个进程都放在有内存和 CPU 上限的临时 systemd unit 里，st-sync 的上限和正式服务相同 |
| `node test/real-check.js [酒馆目录] [端口]` | 对真实酒馆做只读的端到端检查，参数默认读 `/etc/st-sync/env`。登录凭据从 `config.yaml` 读取，不会打印出来，也不写入任何用户数据 |
| `node test/bench.js` | 分块性能测试 |

## 已知限制

- 小于 64KB 的请求不做增量处理。
- shim 只接管注入它的那个页面。页面里的 iframe 和 Worker 各自有自己的 `fetch`，它们发出的请求会按原样转发；访问日志里这类请求没有 `c` 字段。
- 已经压缩过的内容（PNG、JPEG 等）和全新的内容基本省不了流量，比如生成的图片。
- 浏览器缓存被清掉后，第一次加载需要重新下载块，流量和不用 st-sync 时用 gzip 差不多。
- 部署脚本不支持 Docker 里的酒馆；那种情况可以参考“不用 systemd 手动运行”，自己组织容器。
