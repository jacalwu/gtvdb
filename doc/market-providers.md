# Market Data Provider Framework (market provider)

统一行情数据源框架 —— 让 gtv 以**同一套接口**获取任意 provider 的
历史 K 线 / 逐笔数据。provider 只是函数的第一个参数：

```sql
klines('futu',  'HK.00700', '1d', '2024-06-03', '2024-06-07');   -- Futu OpenD
klines('yahoo', '0700.HK',   '1d', '2024-01-01', '2024-03-01');   -- Yahoo Finance
ticks('futu',   'HK.00700', 100);                                 -- 当日逐笔(需盘中 + LV2)
```

新增数据源时**不需要改动任何调用方**：实现一个 trait、注册进 registry，
`klines(...)` / `ticks(...)` 立即按名字生效。

## 设计

```text
crates/gtv-engine/src/market/
├── mod.rs     核心: MarketProvider trait / ProviderRegistry / 统一 Schema /
│              klines(...) / ticks(...) DataFusion 表函数
├── futu.rs    Futu provider  → 内嵌 python bridge → 本机 OpenD (127.0.0.1:11111)
├── yahoo.rs   Yahoo provider → query1/2.finance.yahoo.com v8 chart (带限流重试)
└── python/futu_bridge.py     官方 futu-api SDK 的桥接脚本 (include_str! 打进二进制)
```

### 统一数据模型（所有 provider 输出相同列）

时间戳一律是 **UTC epoch 纳秒 (`Int64`)**，任何 provider 的数据可直接互相
拼接/对齐，无需再转换。

| 数据集 | 列 |
|---|---|
| kline | `provider, symbol, ts, open, high, low, close, volume, turnover, adjclose` |
| tick  | `provider, symbol, ts, price, volume, turnover, direction, sequence` |

`adjclose`：Yahoo 日线及以上有真实值；Futu 默认按 `qfq`（前复权）返回（列填 NaN 表示"已复权/不适用"）。`direction`：`B`/`S`/`N`。

### 接口

```rust
// 所有 provider 只需实现这一个 trait（name 小写唯一）
pub trait MarketProvider: Send + Sync {
    fn name(&self) -> &str;
    fn fetch_klines(&self, req: &KlineReq) -> Result<Vec<RecordBatch>>;
    fn fetch_ticks(&self, req: &TickReq)   -> Result<Vec<RecordBatch>>;
}
```

运行时动态注册（进程内全局 registry，名字不区分大小写）：

```rust
gtv_engine::market::registry().register(Arc::new(MyProvider));
```

SQL / REPL（`register_udtfs` 已在 `GtvContext::new()` 里注册，别名
`market_klines` / `market_ticks`）：

```text
klines(provider, code [, period] [, start] [, end] [, max] [, adjust])
ticks(provider, code [, max])
```

- `period`: `1m 3m 5m 15m 30m 60m 1d 1w 1M 1Q 1Y`（futu 全支持；
  yahoo 到 `1M`）
- `start`/`end`: `YYYY-MM-DD` 闭区间，空串 = 自动窗口（约 `max` 根，
  默认 1000）
- `max`: 显式范围时给 `max` 可截断条数；不给则取全量
- `adjust`: `qfq`(默认前复权) | `hfq` | `none`（仅 futu）
- 纯 Rust 调用：`market::fetch_klines(&req)` / `fetch_ticks(&req)`，与 SQL
  走同一代码路径

## 已内置 provider

| provider | 数据 | 说明 |
|---|---|---|
| `futu` | 历史 K 线 + 当日逐笔 | 通过本机 **OpenD** + 官方 Python SDK；港股/美股/A 股等 |
| `yahoo` | 日/周/月/分钟 K 线 | 免费无 key；`0700.HK`、`AAPL` 风格代码；无逐笔 |

`futu` 依赖（首次调用时懒检查，不阻塞启动）：
- OpenD 已运行并登录（默认 `127.0.0.1:11111`；可用 `FUTU_OPEND_HOST` /
  `FUTU_OPEND_PORT` 覆盖）
- `python3` + `futu-api` 包（`python3 -m pip install futu-api`）
- 覆盖项：`GTV_FUTU_PY`、`GTV_FUTU_TIMEOUT_SEC`

## 调用方式：裸函数简写 vs 标准 SQL

`klines` / `ticks` 是 DataFusion **表函数（table function）**，标准 SQL 里表函数
只能出现在 `FROM` 后面，因此**任何环境通用的写法**是：

```sql
SELECT * FROM klines('futu', 'HK.00700', '1d', '2024-06-03', '2024-06-07');
SELECT * FROM ticks('futu', 'HK.00700', 100);
```

在 gtv **REPL 的 hft 模式**下还额外支持**裸函数简写**（引擎在
`DF_TABLE_FNS` 白名单里匹配到 `klines`/`market_klines`/`ticks`/`market_ticks`
后自动补全成上面的 `SELECT * FROM ...`）：

```text
gtv> klines('futu', 'HK.00700', '1d', '2024-06-03', '2024-06-07')
gtv> ticks('futu', 'HK.00700', 100)
```

| 场景 | 推荐写法 | 原因 |
|---|---|---|
| REPL 随手查 | 裸函数简写 | 少打几个字 |
| 入库：`CREATE TABLE t AS SELECT * FROM klines(...)` | 标准 SQL | 表函数必须在 FROM 里 |
| 加过滤/排序/聚合/join | 标准 SQL | `SELECT ... FROM klines(...) WHERE ...` |
| `remote <host:port> <sql>` 远端执行 | 标准 SQL | 远端走同一 SQL 解析 |
| gtv-server / 脚本 / 其它 SQL 客户端 | 标准 SQL | 无 REPL 简写层 |

> 裸简写只对「整行就是一次函数调用」生效；一旦调用被包在表达式、子查询或
> 多行 SQL 里，就必须写完整的 `SELECT * FROM ...`。

## 使用示例（REPL）

```text
gtv> providers
gtv> klines('futu','HK.00700','1d','2024-06-03','2024-06-07')
gtv> klines('futu','HK.00700','1m','2026-08-03','2026-08-07')      # 分钟级(需行情权限)
gtv> ticks('futu','HK.00700',100)                                   # 盘中逐笔(LV2)
gtv> CREATE TABLE hk700_d AS SELECT * FROM klines('futu','HK.00700','1d','','');
gtv> SELECT count(*), min(ts), max(ts) FROM hk700_d;                # hft 子集外请 full 模式
gtv> klines('yahoo','0700.HK','1d','2024-01-01','2024-03-01')
gtv> klines('yahoo','AAPL','5m','','')                              # 最近 ~1000 根
```

再配上 gtv 原生能力即可继续做时态分析，例如把 K 线灌进表后：
`save hk700_d data/static/xx.parquet`、`tt`、`asof_join`、DataFusion SQL。

## 添加你自己的 provider（三步）

1. 实现 `MarketProvider`（返回统一的 kline/tick Schema）；
2. 构造一个实例并 `market::registry().register(Arc::new(MyProvider))`；
3. 立即可用：`klines('myprovider','XXX','1d',...)` —— 不用改表函数、
   不用改调用方。

> 任何 K 线数据源按上面的列塞进 `kline_schema` 即可入库；逐笔数据若源端
> 只给实时流（如 Futu），可在盘中订阅落盘到 gtv 表，日后再按 `ts` 切片。

## 已知边界

- **日线及以上跨源对齐**：同一交易日，futu 的 bar 时间戳标在 UTC 零点，
  yahoo 标在交易所时区零点（如 0700.HK 的美东凌晨），两个 provider 的 `ts`
  会差几小时。跨源合并日线请先归一到日期再对齐：
  `to_timestamp_seconds(ts)::date`（或自己 `ts / 86400e9` 截断）。分钟/逐笔
  已各自精确换算成 UTC 纳秒，可直接对齐。
- Futu **没有历史逐笔** dump 协议：`ticks('futu', ...)` 只在盘中返回
  （LV2 订阅，单次 ≤ 1000 笔）。
- Yahoo 对分钟数据有回溯窗口限制（`1m` ≈ 30 天、`5m/15m` ≈ 60 天 …），
  更早的区间返回空；连续请求可能触发限流，框架已内置多主机+退避重试。
- 盘中时间换算：Futu 报告的是交易所本地钟（港股/A 股=北京时间 +8，
  美股=美东且自动处理夏令时）；日线及以上保留自然日（UTC 午夜）。
  其余未知名市场按 UTC 标签处理。
- Futu SDK 日志偶发混入 stdout，Rust 侧按 `{"ok"` 起始行稳健提取 JSON，
  不受影响（调试 bridge 请直接跑 `python3 futu_bridge.py ...`）。

## 本地缓存（引擎内置）

`klines(...)`/`md` 已内置**本地缓存优先 + 增量补齐**（`market::cache`），
历史数据只拉一次：

- 每个 `(provider, symbol, period, adjust)` 一个 parquet + meta（覆盖区间）；
  请求区间完全被覆盖 → 直接读盘，不碰 provider；
- 区间外扩 → 只拉**缺失的头/尾段**并合并（按 ts 升序、去重）；
- 复权价 `qfq/hfq` 会被除权重新定价 → 缓存超过 5 天自动整段重拉；
- 日内(分K)且范围含**今天** → 绕过缓存（当日K仍在变动）；
- 目录：`GTV_MARKET_DIR`（默认 `data/market`）；`GTV_MARKET_CACHE=0` 关闭。

```bash
GTV_MARKET_DIR=/path/cache ./gtv          # 指定缓存目录
GTV_MARKET_CACHE=0 ./stock_analysis.sh HK.00700   # 强制现抽
```
