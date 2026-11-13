# cTrader 适配器

cTrader内置于 **tl-store 0.2.1a6**，导入路径为 `tl_store.ctrader`。安装命令、Python／平台范围见[集合说明](../../README.md)。依赖未修改的官方 `nautilus_trader==2.0.0rc7`，独立使用不依赖 TL 或监控服务。

## 配置

`CTraderConfig` 管理预期券商登录号、允许品种、环境、凭据和传输容量。`CTraderDataClientFactory`／`CTraderExecutionClientFactory` 接入官方 NA 工厂接口；只读采集仅需数据工厂的保留会话。

`CTraderCredentials` 按实例接受应用Client ID／Secret和初始OAuth Access／Refresh Token，或已管理的令牌状态路径及grant标识。真实凭据和状态文件不得提交Git。owner／follower共享原有刷新锁及原子状态；follower不初始化或轮换令牌，同一grant不要启动多个刷新owner。

可选TL绑定由 `build_adapter` 提供（adapter API 2／Backtrader interface 1）。简洁参数为 `account`、`client_id`、`client_secret`、`access_token`、`refresh_token`、`practice`。`account` 是正整数**券商登录号**，不是内部cTrader账户ID，认证后解析并校验后者；`practice=True`为Demo，`False`为Live，不能隐式切换环境。

已有托管凭据可使用 `token_state`、`grant_id`、`token_role`，复用同一OAuth状态。可选参数 `trader_id`、`strategy_id`、`economics`、`require_flat_on_connect`、`queue_capacity`、`max_history_records`、`history_lookback_ms`、`request_timeout_ms` 映射原配置，不另建账户声明。未知、冲突、错型参数拒绝；交易仍需账户绑定的费用证据，版本匹配不能绕过该门。

高级 `CTraderStoreConfig` 组合 `CTraderConfig`、官方 `LiveNodeConfig`、可选 `StrategyConfig`、本地状态目录和权威账户provider。数据／执行客户端各配置一个 `CTRADER`。高级配置与简洁参数互斥。

公开Store的导入和启动方式由所选TradeLearn发行版决定，见[版本兼容](../../README.md#use-with-tradelearn)；不能把候选语法当成旧wheel已支持。

## 行情与执行

当前支持元数据解析、报价、账户观察、执行报告、普通Market IOC、撤单、指定hedged持仓平仓，以及下述已观察盘口。Limit、Stop／StopLimit、追踪和Bracket／OCO关联单仍有实现缺口，不代表券商协议不支持；不隐式启用NA仿真。

TL行情编译使用准确的券商品种名和元数据确认标的及资产类别。`timeframe`／正整数 `compression` 支持Seconds、Minutes、Days、Weeks、Months、Years；MicroSeconds必须是整毫秒。`price`为小写 `bid`／`ask`／`mid`，时间边界由固定NA内部聚合器负责。此路径不接受LAST、外部bar、tick-count或volume bar，不补空秒、不隐式回填历史。报价数量缺失时编码为0，并非观察到的成交量。

持仓及费用必须来自权威事实，不能用余额替代权益、缺费补0或猜测仓位。`require_flat_on_connect` 是明确准入政策；指定平仓须核归属、路由和数量，普通反向单不自动等于hedged平仓。

## 构建与验证

常规构建、测试和平台资格统一见[根目录说明](../../README.md#build-and-test)。保持Cargo.lock、许可、原生身份和RECORD一致，不给旧二进制换版本标签。Windows／Linux ARM64构建成功不等于实际运行验证。

可选交叉构建工具 `tools/export_target_python_abi.py` 及 `tools/build_release.py --target ... --target-python-abi ... --target-python-dir ...` 使用校验过的目标ABI导出，不用宿主ABI冒充目标；仍需对应工具链和目标平台安装检查。外置锁定vendor树使用 `--vendor-dir` 重映射源路径。普通用户优先安装wheel。

<a id="read-only-capture-seam-021a5"></a>
## 只读采集接口（0.2.1a5起）

仅数据集成可使用真实 `CTraderDataClientFactory(options)`，保留 `await factory.session.acquire(execution=False)`；不需要执行客户端、交易引擎或执行journal。保持原生事件reader健康，结束时正常释放租约，不为每次采样新建会话。OAuth owner／follower、刷新锁、原子令牌状态与账户绑定不变。

`await transport.state()` 返回原生 `session_id`、`epoch`、`revision`、`mono_ns`、`utc_ns`。不透明身份在保留连接内稳定，新连接更换身份；时钟为原生本地观察时点，不是券商事件时间。采样前后检查epoch／revision及健康状态，死连接报错，不在此调用内部重连；Python不能合成替代这些字段。

```python
page = await transport.request("orders", {
    "fromTimestamp": start_ms,
    "toTimestamp": end_ms,
})
```

`"deals"`同样返回原始券商分页并保留 `hasMore`。时间为非负整数，满足 `start <= end <= 2147483646000`；每页受 `max_history_records` 限制，超量或缺少布尔完整性标记拒绝。单页不代表整个历史区间完整，消费者仍须执行有界拆分／重试及原始事实校验。

使用既有Orders／Deals Open API消息，不另建协议。字符串白名单不含变更操作；`execution=False` 下直接调用原生 `mutate` 也会在journal或交易I/O前拒绝。该接口不增加Engine策略方法。

TL版本锁见[版本兼容](../../README.md#use-with-tradelearn)。公开wheel、只读观察及离线验证不证明真实账户具有深度或交易权限。

<a id="standard-observed-depth-a6"></a>
## 标准已观察盘口（0.2.1a6）

`CTraderDataClient` 实现 `_subscribe_book_deltas`、`_unsubscribe_book_deltas`、`_subscribe_book_depth`、`_unsubscribe_book_depth`，复用同一factory、`execution=False, market=True`租约和reader。

订阅要求 `BookType.L2_MBP`；`depth=None/0`选择已观察档位（输出上限1000），正整数选择前缀。最多10个品种，每品种10000原生条目，总容量100000。同品种不同订阅模式共享原生请求／订单簿，并发模式等待同一真实ACK；最后一个模式退订时发送一次原生退订。断连会退订并释放租约，未知退订失败不能算ACK成功。

原生 `DepthBook`／`depth_wire` 唯一维护quote ID、增改删、同价聚合、交叉／溢出／容量及身份校验；Python只是薄出口，不维护第二个簿。`OrderBookDeltas` 按已观察视图输出成组CLEAR＋ADD及MBP／最终记录标记；深度快照输出固定NA2rc7实际类型 `OrderBookDepth`，不使用该版本不存在的 `OrderBookDepth10`。二者沿标准 `_handle_data`／`_output.data` 输出。

### 缺失事实与时钟

协议2155没有源时间戳、源序号、全市场完整快照或客户订单数：

- `ts_event=0`、`sequence=0`表示不可用，不是已知源时间／序号。
- `ts_init`保留原生接收UTC时钟，不改成Python发布时间。
- 消费者源时间用null、`complete=false`；不添加完整 `F_SNAPSHOT` 声明。
- 订单数0表示元数据不可用，不代表真实没有订单。
- 原始价格／100000、数量／100通过Decimal及标的精度准确转换，不允许舍入。
- 深度事件不刷新QuoteTick的来源时钟或新鲜度。

传输epoch变化、错误身份／方向／价格／数量、协议／容量或精度失败会使原生簿失效，并沿既有失败路径关闭准入。新连接必须建立新簿，不隐藏重连或重放旧epoch。重复有效更新替换同quote ID，无法证明的重复删除保持失败关闭。消费者仅将标准已观察快照投影到当前视图；raw2155旁路只作证据，不再累计一份订单簿。

### 拒绝与账户权限

关联的只读50／2142错误只保留原始有界 `errorCode`，通过 `tl_store.ctrader.errors.CTraderRequestRejected.error_code` 提供。排除描述、账户及订单正文；缺失或不合法的code为None，不猜测错误码。其他原生错误保留原类型／原因。

真实账户／品种的深度权限、2157／2155或拒绝码证据须由授权只读消费者单独验证，不能用报价成功、工厂构造或离线测试替代；验证不使用交易订单。
