# TL-Store

**TL-Store 是 TradeLearn 的适配器集合**，负责接入不同的交易平台与行情来源。适配器按组件独立维护，用户通过 TradeLearn 的 Store 接口配置和使用。

当前发行包为 `tl-store`，首个内置组件是 **cTrader**，导入路径为 `tl_store.ctrader`。当前支持的框架为 NautilusTrader，用于承接执行与行情能力；后续适配按组件扩展。

<a id="use-with-tradelearn"></a>
## 接入 TradeLearn

TradeLearn 负责可选依赖的版本锁定：

```sh
python -m pip install "trade-learn[nautilus,tl-store]==0.3.2"
```

**正式 TradeLearn 0.3.2 选择 Store 0.2.1a4，而不是 a6。** 独立安装 a6 不等于升级并验证了该固定组合。请使用所选 TL 发行版对应的版本和导入方式；新的 `bt.stores.NautilusStore`、普通 `bt.Strategy` 透明接线仍属 TL 后续候选，不能套用到旧正式版。

a5新增原生只读观察状态及有界订单／成交分页，a6新增标准 NA 盘口订阅和只读拒绝码。适配器独立升级不要求未变化的 TradeLearn 代码同时发行。

## 独立安装

当前已发布版本为 **0.2.1a6（Alpha）**，使用 CPython **3.13 或 3.14**：

```sh
python -m pip install "tl-store==0.2.1a6"
python -m pip check
```

当前实现依赖固定为未修改的官方 `nautilus_trader==2.0.0rc7`。构建和离线测试通过，不代表真实券商账户已获得全部能力或交易权限。

| 平台 | 已发布 wheel | 验证范围 |
| --- | --- | --- |
| macOS Apple Silicon | CPython 3.13 / 3.14 | 已安装运行离线测试；3.13完成随包对应源码离线重建 |
| Linux x86_64，glibc 2.34+ | CPython 3.13 / 3.14 | 已安装运行离线测试；3.13完成随包对应源码离线重建 |
| Windows x64 | CPython 3.13 / 3.14 | 仅构建通过，尚无同等运行验证 |
| Linux ARM64，glibc 2.34+ | CPython 3.13 / 3.14 | 仅构建通过，尚无同等运行验证 |

本次发行矩阵不包含 macOS Intel wheel。

## 当前框架：NautilusTrader

```python
from tl_store import ctrader
```

除通过TL接入外，当前适配器也可独立用于NautilusTrader，不要求安装TL或监控服务。适配器提供 `CTraderDataClientFactory`、`CTraderExecutionClientFactory`、账户配置及原生扩展 `tl_store.ctrader._native`。通过官方 NA 客户端工厂接口装配；仅采集数据时使用数据工厂保留的只读会话，不必创建执行客户端。

配置、能力限制、只读采集和盘口接口见 [cTrader 使用说明](adapters/ctrader/README.md)。

## 能力与边界

- 券商元数据解析、报价、账户观察、执行报告、普通 Market IOC、撤单及指定持仓平仓。
- 复用原生 `DepthBook` 输出标准 NA 已观察盘口；真实账户／品种的深度权限须单独核验。
- 时间 K 线使用官方 NA 内部聚合，不另建聚合或执行循环。
- Limit、止损、追踪和关联单仍有适配器实现限制，不能从 NA 枚举或券商协议能力推断已支持。
- 缺少的盘口源时间、完整性和报价数量不伪造为真实市场事实。

<a id="build-and-test"></a>
## 构建与测试

使用 Rust **1.97.0**、CPython 3.13/3.14、maturin **1.13.1**；发行元数据以根目录 `pyproject.toml` 为准。

```sh
python -m pip install "maturin==1.13.1"
python tools/check_release.py
python tools/build_release.py --out raw-dist
cargo vendor --locked --versioned-dirs \
  --manifest-path adapters/ctrader/native/Cargo.toml recipient-vendor
```

逐个处理实际生成的 wheel，将占位文件名替换为真实名称：

```sh
python tools/finalize_wheel.py raw-dist/<wheel>.whl dist/<wheel>.whl \
  --vendor-dir recipient-vendor
python tools/check_release.py --wheel dist/<wheel>.whl
```

构建器启用优化和编译路径重映射。Linux必须通过 manylinux 2.34 审计，不能靠重命名伪造 ABI 兼容。CI构建八个 wheel，在 macOS ARM64／Linux x86_64运行离线测试，并在其3.13任务中离线重建对应源码；Windows／Linux ARM64只证明构建。

独立测试需先安装已构建的原生扩展：

```sh
python -m pip install "pytest>=9,<10"
python -m pytest adapters/ctrader/tests \
  --ignore=adapters/ctrader/tests/test_store.py
cargo test --locked --manifest-path adapters/ctrader/native/Cargo.toml --lib
```

`test_store.py` 另需兼容的 TL adapter API 2／interface 1。测试采用合成身份、回环传输和确定性事件，不连接真实账户或下单。

发行工作流读取成功且源码一致的 CI 产物，通过 GitHub OIDC 发布 PyPI；整理文档不要求重新构建或覆盖已发布 wheel。

## 使用说明

项目仍在持续完善，建议先在测试环境验证，再根据实际需求使用。请妥善保管账户与凭据，避免提交敏感信息。

许可见 [LICENSE](LICENSE)、[COPYING](adapters/ctrader/COPYING)、[COPYING.LESSER](adapters/ctrader/COPYING.LESSER) 和 [NOTICE](adapters/ctrader/NOTICE)。
