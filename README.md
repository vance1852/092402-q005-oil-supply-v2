# 油气供应韧性与现场准入服务

本项目是一套可直接运行的 Python 服务端系统，用于记录原油基准报价、油田与终端设施、输送线路、库存批次、日提名和供应情景，并保留油田巡检机器人统计准入流程。系统面向价格连续波动、关键输油线路恢复、库存调拨和现场设备验证同时发生的运营环境，让调度、风险和审计人员在同一个 SQLite 数据库中获得可追溯结论。

供应调度子域提供以下能力：

- 原油基准报价按交易日和来源修订登记，历史版本不会被覆盖；
- 油田、储罐、终端与炼厂设施建档，线路保存日能力、在途时间和损耗规则；
- 线路停运或降容事件按 UTC 时间区间生效，日分配会计算实际可用能力；
- 库存批次保留油品、牌号、数量、单位成本和接收时间，可计算加权库存成本；
- 交接班盘点会话冻结开始时的库存与在途快照，逐罐测量按油品汇总差异；容差内由当班人员结清，超容差必须由另一名风险人员选择调整、拆分调查或拒绝，调整幂等并保留前后余额与签署链；
- 托运提名支持载荷级幂等、优先级分配、库存扣减和在途交接；
- 供应情景保存价格变化、线路能力变化和需求变化，审批后产生可重放的确定性结果；
- 关键写操作进入哈希串联审计日志，可离线验证事件顺序和内容完整性。

现场准入子域位于 `robot_trials` 包，负责油田巡检机器人的设备构建登记、不可变试验协议、观测分片导入、异常观测复核、统计任务租约、准入决定和审计报告。该子域不连接机器人硬件，只处理已经结构化的试验记录。

## 目录

- `src/oil_supply/`：报价、设施、线路、库存、提名、供应情景、HTTP API 与离线验收；
- `src/robot_trials/`：油田巡检机器人试验与统计准入；
- `fixtures/`：现场准入演示协议和结构化观测；
- `tests/`：核心规则、错误边界、API 和端到端验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 无第三方运行依赖

在依赖已经准备好的容器中安装：

```bash
python3 -m pip install --no-index --no-deps .
```

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

测试使用内存数据库和临时目录，不访问公网，也不会启动常驻服务。

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m oil_supply.acceptance --workspace .
```

该命令会在内存数据库中登记六个交易日的布伦特报价，创建油田、终端和输送线路，完成库存入账、提名分配、发运、交接班盘点结清及供应情景分析，最后输出一行 JSON。成功时退出码为 `0` 且 `status` 为 `ok`。

现场准入子域也保留独立验收入口：

```bash
PYTHONPATH=src python3 -m robot_trials.acceptance --workspace .
```

## HTTP 服务

```bash
PYTHONPATH=src python3 -m oil_supply.api --database oil_supply.sqlite3 --host 127.0.0.1 --port 8080
```

健康检查为 `GET /health`。除健康检查外，请求通过 `X-Actor-Id` 携带操作者编号。可用接口覆盖报价、设施、线路、停运事件、库存批次、提名、能力分配、发运、供应情景和审计链。服务重启后，SQLite 中的业务状态和历史版本会继续保留。

### 交接班盘点接口

- `POST /stocktakes`：当班调度开启会话，冻结该设施当前库存批次与在途量快照（含 `snapshot_sha256`），请求体含 `session_id`、`facility_id`、`tolerance_percent` 和可选 `products`；同一设施同时只能有一个 `open` 会话。
- `POST /stocktakes/{id}/measurements`：逐罐提交测量（`tank_id`、`product`、`measured_barrels`、可选 `measured_at`），系统按油品汇总并计算 `delta_barrels`、`variance_percent` 与 `within_tolerance`；同一会话同一储罐只能测量一次，迟到补录会推进油品行 `revision`。
- `POST /stocktakes/{id}/settle`：结清一个油品，必须携带 `expected_revision` 与 `idempotency_key`。容差内可由当班调度 `adjust` 或 `reject`；超容差必须由非开启人的风险角色选择 `adjust`、`split_investigation` 或 `reject`。账面无批次的盘盈调整需要 `target_lot_id` 指定承接批次。
- `POST /stocktakes/{id}/investigation`：风险人员对 `investigating` 油品给出最终 `adjust`/`reject` 结论。
- `POST /stocktakes/{id}/close`：所有油品结清后关闭会话。
- `GET /stocktakes/{id}` 与 `GET /stocktakes?facility_id=&state=`：查询会话详情与列表。油品行状态明确区分为 `unmeasured`（未测量）、`pending_review`（待复核）、`adjusted`（已调整）、`rejected`（已驳回）和 `investigating`（拆分调查中）。

盘点期间相关批次仍可正常发运；调减按批次当前余额比例分摊，并以批次版本号检测并发改写。结清调整幂等（重放同一 `idempotency_key` 返回同一结论），每条调整保留 `balance_before`、`balance_after`、原因和签署人，并进入哈希串联审计链。
