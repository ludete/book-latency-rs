# book-latency-rs

独立的 Rust 订单簿接收延迟测试工具，对应 `entropy-arb/tools/book_latency.py`。
只读取公开市场元数据与 WebSocket 订单簿，不需要 API key，不读取 `.env`。

```bash
cargo build --release --locked
./target/release/book-latency-rs --venues hyperliquid lighter gate --symbol BTC --duration 60 --percentiles 50 90 99
./target/release/book-latency-rs --venues entropy tradexyz lighter-rh arcus ondo --symbol SNDK --duration 60 --pxx 90 95 99.9
./target/release/book-latency-rs --venues gate entropy --symbol BTC --market entropy=SNDK --duration 30
```

需要 Rust 1.85+、网络连接和同步的系统时钟。首次构建需要下载 Cargo 依赖。

| 场所 | 市场/订阅 | 订单簿时间戳 |
| --- | --- | --- |
| `hyperliquid` | 原生永续；`l2Book`，`fast=true` | `data.time`，毫秒 |
| `entropy` | Hyperliquid `io` DEX；同上 | 同上 |
| `tradexyz` | Hyperliquid `xyz` DEX；同上 | 同上 |
| `lighter` | Lighter 主网；`order_book/{market_id}` | `order_book.last_updated_at`，微秒 |
| `lighter-rh` | Lighter Robinhood；同上 | 同上 |
| `gate` | USDT 永续；`futures.order_book`，20 档、间隔 `0` | `result.t`，毫秒 |
| `arcus` | `l2Orderbook`，100 档 | `contents.timestamp`，微秒 |
| `ondo` | `depthBooksPerps`，100 档 | `data[].time`，带时区 RFC 3339 |

默认尝试全部 8 个场所，品种 `BTC`、时长 60 秒、分位数 p50/p90。部分场所可能
没有该品种，会单独显示错误。`--market VENUE=SYMBOL` 可重复，覆盖选定场所的品种；
例如 `gate=BTC_USDT`、`arcus=SNDK-USD`、`ondo=SNDK-USD.P`。
Hyperliquid 的 DEX 前缀由程序根据场所补全，传入 `SNDK` 即可。
`--percentiles` 与 `--pxx` 等价，接受 0–100 的有限数值，去重排序并始终包含 p50。

输出列：`venue`、`market`、`n`（样本数）、`invalid`（无效消息数）、`negative`（负值数）、
`mean_ms`、指定的 `pXX_ms`、`status`。无样本显示 `N/A`。

测量值为 **本地收包时间 − 交易所订单簿时间戳**，单位毫秒，收包时间在 JSON 解码前记录。
差值包含盘口年龄、交易所批量推送、网络传输、本地调度和时钟偏差，不能直接当作纯网络延迟。
负值保留；初始快照和相同时间戳的更新均计入，心跳和其他市场的消息排除。
分位数使用排序后的线性插值；Ondo 保留原始纳秒字段精度（最终计算使用 f64）。
不重建或验证增量订单簿。

所有场所先并发加载元数据（单次 HTTP 最多 10 秒，每个场所总计最多 15 秒），
再在共同的 `--duration` 窗口内并发连接和采集，连接时间计入窗口。
WebSocket 连接超时 10 秒；应用心跳间隔 5 秒（Lighter 响应服务器 ping），
另每 15 秒发送 WebSocket ping，等待 pong 最多 15 秒。消息大小上限 8 MiB。
连接失败或中断不重试，保留已收集样本；时间到或 Ctrl-C 时取消挂起操作并释放连接。
任一家失败不会终止其他场所。
WebSocket 握手拒绝会显示 HTTP 状态和最多 512 个字符的响应正文（例如服务端的地区限制提示）。

退出码：全部 `OK` 为 0；任一家 `ERROR`、`INVALID` 或 `NO_DATA` 为 1；参数错误为 2；
Ctrl-C 输出已有统计并返回 130（包括元数据加载期间）。

HTTP 和 WebSocket 均直连，不读取代理环境变量；请在能直接访问交易所的网络上运行。
样本保存在内存中，适合有限时长的比较，内存用量随样本数增长。使用 `--release` 进行测量。
交易相关的签名、精度规则和账户接口不属于本工具的功能。

验证：

```bash
cargo fmt --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
```

测试覆盖全部场所的市场映射、原 Python 工具的盘口样本、时间戳单位与时区、
负值/重复/无效样本、分位数与状态输出，以及本地 WebSocket 订阅、心跳、断连和超时。
测试无需交易所网络；本地 WebSocket 测试需要允许监听回环地址。
