# RabbitMQ / Kafka 连接器交接文档

更新日期：2026-11-14（Asia/Shanghai）。本文供切换到有真实 MQ 测试条件的环境后，由其他 Agent 接续开发、排障和验收。

## 1. 接手时先看这些信息

- 当前分支：`dev_mq`。
- 写本文时 HEAD：`fbe1d911ad07c0d54b34e166d0594d9a6fd4bbcd`。
- **本轮功能修改尚未提交。两个新模块等文件仍是未跟踪文件，不能只 checkout 分支就认为已迁移完整。请连同已修改文件、新增源码及本文一起保存或迁移。** 本轮没有创建提交、推送或发布远程版本。
- 当前状态：功能代码、离线验证、可执行 JAR 和本地发行包已经完成；**真实 RabbitMQ / Kafka 验收尚未开始**。
- 用户明确要求：当前环境没有 Broker，先实现功能，不连接真实 Kafka/RabbitMQ；用户稍后提供另一套测试环境。不要在拿到测试条件前自行连接未知 MQ、安装 Broker 或启用真实 Broker CI。
- 当前版本号仍为 `0.1.1`；没有因本轮功能而自动升级版本。

接手后先执行：

```text
git branch --show-current
git status --short
java -version
mvn -version
```

确认两个模块及其测试源码均在工作区，再开始后续工作。不要重置或覆盖已有未提交改动。

## 2. 预期目标及已经确定的范围

目标是让开发、调试过程中的 Agent：

1. 查看 MQ 的消息数量、积压、消费者及消费进度。
2. 排查 RabbitMQ Ready / Unack、Kafka 消费组 lag 和分区分配。
3. 查看有限消息样本，并用 messageId、correlationId、trace headers、Kafka partition/offset 与应用日志关联。
4. 直接投递测试事件，或临时代替应用接收消息，验证局部业务链路，无需让应用完整运行上游业务流程。

实现分为 `omni-connect-rabbitmq`、`omni-connect-kafka` 两个独立模块，共 **17 个新工具**。两个模块依赖现有 core，不新增 `mq-common` 模块，继续发布同一个可执行 JAR。

已确认的产品和技术决策：

| 项目 | 决策 |
| --- | --- |
| RabbitMQ 接口 | Management HTTP API 必选，AMQP 可选 |
| RabbitMQ 认证 | 用户名、密码 |
| Kafka 认证 | `PLAINTEXT` 或 `SASL_PLAINTEXT`，SASL 为 `PLAIN` |
| Kafka 业务消费组 | 可以显式指定业务 groupId；提交位点默认关闭 |
| 消费生命周期 | 有限条数、有限等待时间，单次调用内结束 |
| 配置 | YAML 启动激活或运行时 connect；默认禁用启动激活 |
| 工具暴露 | connect 始终可见；成功激活后注册业务工具 |
| 连接切换 | 先验证新连接，失败保留旧连接；运行时配置仅在内存中 |
| 测试安排 | 本轮只做离线验证，真实 Broker 验收延后至用户提供的新环境 |

首版不包含：TLS、SCRAM、Kerberos、OAuth、Schema Registry、资源创建删除、purge、消费组位点重置、持续订阅、跨调用 ACK/NACK、业务处理逻辑，以及 Redis Sentinel/Cluster 扩展。

原计划提过 CI 容器验收，**后来已由用户改为延期真实测试**。本轮没有引入 Docker/Testcontainers，也没有新增真实 Broker CI 工作流；不要把旧计划当成已经完成的工作。

## 3. 当前已经完成的实现

### 3.1 新增模块和入口

| 位置 | 职责 |
| --- | --- |
| `omni-connect-rabbitmq/` | RabbitMQ 模块、管理 HTTP 请求、AMQP 投递/消费和离线测试 |
| `omni-connect-kafka/` | Kafka Admin/Producer/Consumer 操作和离线测试 |
| `omni-connect-core/.../MessageSupport.java` | 结构化结果、脱敏错误、正文预览、大小限制和操作期限辅助方法 |
| `omni-connect-app/.../MqMcpOfflineTest.java` | 通过真实 MCP HTTP 会话和客户端替身验证 MQ 激活及工具注册 |

每个新模块都有 `*ConnectorModule`、`*Clients`、`*Runtime`、`*Tools` 四个主要类：

- `ConnectorModule` 负责配置读取、connect、验证候选客户端并激活。
- `Clients` 是可注入客户端创建边界，生产创建真实客户端，测试使用替身。
- `Runtime` 执行管理、观察、投递和消费操作，管理资源关闭。
- `Tools` 提供 MCP schema、参数和副作用注解。

客户端依赖由现有 Spring Boot BOM 管理，不引入 Rabbit/Kafka Spring Boot Starter。RabbitMQ 管理请求使用 JDK HttpClient；AMQP 使用 RabbitMQ Java Client；Kafka 使用 Kafka Java Client。

### 3.2 RabbitMQ 工具：9 个

| 工具 | 当前功能 |
| --- | --- |
| `rabbitmq_connect` | 验证管理接口、vhost，以及可选 AMQP 连接；激活/替换连接 |
| `rabbitmq_list_queues` | 按名称筛选、分页查看队列类型、Ready、Unack、消费者数 |
| `rabbitmq_inspect_queue` | 队列数量、消息速率、arguments、policy、effective policy |
| `rabbitmq_list_bindings` | 按 queue 或 exchange 查询绑定 |
| `rabbitmq_list_consumers` | 消费者、ACK 模式、prefetch、Channel 标识 |
| `rabbitmq_inspect_channel` | Channel 范围的 Unack、prefetch、所属连接与客户端名称 |
| `rabbitmq_sample_messages` | 从 Ready 消息取样并重新入队，返回有限正文 |
| `rabbitmq_publish_message` | AMQP 投递，使用 mandatory 和 publisher confirm |
| `rabbitmq_consume_messages` | AMQP 有限拉取并发送手动 ACK，返回执行进度 |

仅配置管理接口时也会注册全部 8 个业务工具；AMQP 投递、确认消费返回 `CAPABILITY_NOT_CONFIGURED`。如果配置了 AMQP 而验证失败，候选连接整体不生效。

### 3.3 Kafka 工具：8 个

| 工具 | 当前功能 |
| --- | --- |
| `kafka_connect` | 验证集群元数据访问，激活/替换连接 |
| `kafka_list_topics` | Topic 筛选、数量限制，默认隐藏内部 Topic |
| `kafka_inspect_topic` | 分区、leader、replicas、ISR、起始/末端 offset |
| `kafka_list_consumer_groups` | 消费组及可获得的状态 |
| `kafka_inspect_consumer_group` | 成员、分区分配、提交位点、末端位点、lag |
| `kafka_read_records` | 按分区和位置读取，不加入业务组、不提交位点 |
| `kafka_publish_record` | 向已有 Topic 投递，等待并返回 partition/offset/timestamp |
| `kafka_consume_records` | 使用显式 groupId 有限消费，可选提交已返回记录的位点 |

精确参数和调用示例见 [README.md](README.md) 中的 `RabbitMQ and Kafka (development diagnostics)` 小节。

### 3.4 装配、配置和文档

- 根 `pom.xml` 注册两个模块，应用模块加入依赖。
- `application.yml` 新增 `omni.rabbitmq`、`omni.kafka`，默认均 `enabled: false`。
- 根 Surefire 配置以 Java agent 方式加载 BOM 管理版本的 Mockito，避免 JDK 25/受限桌面环境动态 attach 卡住。
- README、Agent Guide 已补充工具、配置、消息副作用和验证边界。
- 现有工具数量及管理工具过滤测试已更新。

工具数量约定：

| 激活状态 | 工具数 |
| --- | ---: |
| 所有连接器未激活 | 8 |
| 原有四个连接器激活，MQ 未激活 | 28 |
| 仅 RabbitMQ 和 Kafka 激活 | 23 |
| 全部六个连接器激活 | 43 |

## 4. 必须保留的消息语义

这些是后续修复、验收时的约束，不应为通过测试而弱化。

- RabbitMQ sample 只能读取 Ready 消息，不能读取已经投递的 Unack 正文；重新入队仍有副作用，不能标记为纯只读。
- Channel Unack 可能覆盖多个队列，不能直接归因于某一个队列。
- RabbitMQ publish 区分已确认、无法路由、Broker 拒绝和结果未知；confirm 不代表业务处理成功。
- RabbitMQ consume 在本次调用内拉取并 ACK；结果标记 `ackStatus=SENT`。ACK 没有 Broker 回执，不能改成“业务已完成”或“Broker 已确认 ACK”。
- 超出结果大小上限的 RabbitMQ 消息不得 ACK；中途失败保留消息及已发送 ACK 的进度。
- Kafka read 使用手动分区分配和定位，不加入业务消费组，不提交位点。
- Kafka consume 关闭自动提交。只有显式 `commit=true` 才提交各分区“实际返回记录的最后 offset + 1”，不得直接提交 poll 后位置而跳过未返回记录。
- 加入业务 groupId 可能触发 rebalance；提交会改变该组后续消费进度。MCP 接收消息不会执行应用业务逻辑。
- Kafka lag 是 offset 距离，不是业务完成证明，也不保证等于精确消息条数。缺失、越界位点不能伪造为 0 lag。
- Kafka Producer 使用 `acks=all` 和幂等性；不能据此承诺跨 MCP 工具调用去重。
- 不盲目重试结果未知的投递、消费或提交。
- 不自动建 Topic/交换机/队列；真实验收资源由测试环境预先准备。
- 返回已有 messageId/correlationId/headers、Kafka 位置信息；不自动改写正文或添加业务信封。
- 不返回或记录密码、原始 JAAS 内容。状态中的 AVAILABLE 表示激活验证结果，不是持续健康监测。

## 5. 当前验证结果及其证据

最后一轮本地全量构建：

```text
mvn --batch-mode --no-transfer-progress verify -DskipTests=false
```

结果：**BUILD SUCCESS**。Surefire 报告合计 296 项，失败 0、错误 0、跳过 1，即 295 项通过；这是整个工程的统计，不是新增 MQ 测试数。

离线测试主要位于：

- `omni-connect-core/src/test/java/xyz/fz/omni/core/MessageSupportTest.java`
- `omni-connect-rabbitmq/src/test/java/xyz/fz/omni/rabbitmq/RabbitMqOfflineTest.java`
- `omni-connect-kafka/src/test/java/xyz/fz/omni/kafka/KafkaOfflineTest.java`
- `omni-connect-app/src/test/java/xyz/fz/omni/app/MqMcpOfflineTest.java`

覆盖本地 HTTP 管理接口 fixture、客户端替身、消息限额与编码、错误脱敏、连接失败保留原目标、confirm/return、ACK 进度、Kafka 提交位点、SASL 配置不继承、观察读取不加入组，以及跨 MCP 会话的动态工具注册和全部 43 个工具可见性。

另已执行通过：

```text
python tests/test_release.py
python scripts/package_release.py v0.1.1
git diff --check
```

发布烟测覆盖 HTTP 鉴权、启动/重启/停止、三个 stdio 子进程、EOF、进程隔离和发行布局。Windows 运行该脚本需要读取 CIM 进程信息；当前沙箱内曾被拒绝，随后在允许读取 CIM 的本地执行环境中通过。不要因这类权限失败修改产品连接逻辑或直接跳过验收。

本机证据和产物（通常被 Git 忽略，新环境需重新生成）：

- `target/mq-verify.log`
- `target/mq-release-tests.log`
- 各模块 `target/surefire-reports/`
- `target/release/omni-connect-mcp/omni-connect-mcp.jar`
- `target/dist/omni-connect-mcp-v0.1.1-windows.zip`
- `target/dist/omni-connect-mcp-v0.1.1-linux.tar.gz`

**没有连接过真实 RabbitMQ 或 Kafka。替身验证不等于已验证真实协议交互、权限、指标、重平衡、故障恢复或版本兼容。**

## 6. 新环境尚需完成的工作

### P0：真实 Broker 验收与必要修复

1. 确认真实 Broker 版本、地址、网络可达性和用户授权测试资源。
2. 在新环境重跑离线构建，确认迁移完整。
3. 建立独立、可清理的测试 vhost/队列/交换机、Topic、消费组；不要用业务资源做破坏性确认消费。
4. 将下表关键场景补成可重复执行的集成测试。接入现有 Broker 还是容器，由新环境条件决定；不预设一定有 Docker。
5. 修复真实交互发现的问题后，重跑离线回归和发布烟测，再记录 Broker/客户端版本、命令、结果和剩余问题。

| 领域 | 必验场景 |
| --- | --- |
| 认证 | RabbitMQ 正确/错误密码、vhost 权限；Kafka 无认证、PLAIN 正确/错误凭据及资源 ACL |
| RabbitMQ 管理 | 队列名/vhost 路径编码、管理反向代理前缀、分页/过滤、缺失指标、权限不足 |
| RabbitMQ 积压 | 准备 Ready 消息及真实消费者持有的 Unack；验证队列、消费者、Channel 和应用连接的关联 |
| RabbitMQ 取样 | 取样后重新可用、正文/headers/截断信息正确；记录投递状态和顺序副作用 |
| RabbitMQ 投递 | 默认交换机、普通交换机、可路由/不可路由、不存在交换机、confirm nack/超时/断线 |
| RabbitMQ 消费 | ACK 后消息减少；结果大小限制不 ACK 隐藏消息；中途失败、Channel 关闭及未 ACK 消息重新入队 |
| Kafka 元数据 | 分区、leader、ISR、保留边界、消费组成员和 lag；未提交/越界/空消费组 |
| Kafka 观察 | earliest/latest/offset/timestamp，空 Topic、压缩/有 offset 间隙的 Topic、正文/重复 headers；业务提交位点不变 |
| Kafka 投递 | key、headers、显式分区及返回 offset 可再次读取；不存在 Topic 不自动创建 |
| Kafka 消费 | 调试组与业务测试组；默认不提交、显式提交、响应截断不跳过记录、rebalance、提交失败/结果未知 |
| 生命周期 | 候选连接失败保留旧连接；跨会话共享；操作期间切换；断线和关闭时资源/线程释放 |
| MCP/发布 | connect 后真实客户端刷新工具；YAML 启动激活；HTTP 与 stdio；8/23/28/43 工具数量 |

首版原定验收目标是 RabbitMQ 4.x、Kafka 3.9/4.x；当前没有任何真实版本的兼容性验收结论。若新环境版本不同，明确记录实际范围，不默认为已经支持。

### P1：接续代码审查应优先核对的实现边界

以下是由当前源码可见、需要结合真实测试确认或改进的点，不表示已有线上故障：

- RabbitMQ 管理 HTTP 的连接超时目前固定 5 秒；`omni.rabbitmq.timeouts.connect-ms` 实际用于 AMQP，尚未统一作用到管理 HTTP。
- `MessageSupport` 有 30 秒期限，若干等待使用剩余预算；RabbitMQ RPC/Channel close、Kafka poll/send/close 等客户端调用并非全部由同一预算严格约束。验证阻塞或断线时的总耗时，不要宣称已有严格端到端 30 秒保证。
- RabbitMQ Java Client 当前关闭自动恢复。连接断开后可能需要重新 connect；首版不承诺自动故障切换。
- RabbitMQ consumers/bindings 部分接口使用限大小的完整响应再本地分页；管理响应上限为 2 MiB。检查大 vhost 下的实际接口分页和过滤行为。
- Kafka 当前使用的 `listConsumerGroups` / `ConsumerGroupListing` API 在 BOM 管理的客户端中已有弃用警告；评估后续替换时保持目标 Broker 兼容范围。
- Kafka consume 的提交失败保留记录和 attempted offsets；poll 阶段中途异常的部分读取结果处理仍需审查。
- 消息响应大小限制主要针对消息集合，外层包装、元数据及超大 headers 的整体限制应验证；检查截断标志是否足够清楚。
- 当前错误分类与结果未知处理较保守，需要真实权限、协议关闭和超时场景验证；优先保留已执行进度，不能为了返回“成功”而隐藏不确定性。
- 现有离线测试不是所有参数、异常和并发组合的穷举。真实修复需增加有意义的回归用例。

TLS、SCRAM 等范围外能力不是本轮遗留验收任务；只有用户另行扩展范围时再实现。

## 7. 新环境操作入口和验收记录要求

运行要求为 JDK 25+、Maven、Python 3。先保持 `application.yml` 中两个 MQ 的 `enabled: false`，避免应用启动时意外连接。

离线基线：

```text
mvn --batch-mode --no-transfer-progress clean verify -DskipTests=false
python tests/test_release.py
```

获得测试环境后可使用 MCP connect 工具激活。以下均是待替换占位信息，不是当前可用的服务器或凭据：

```json
{"name":"rabbitmq_connect","arguments":{"managementUrl":"http://<rabbit-host>:15672","username":"<test-user>","password":"<test-password>","virtualHost":"<test-vhost>","amqpAddresses":"<rabbit-host>:5672"}}
{"name":"kafka_connect","arguments":{"bootstrapServers":"<kafka-host>:9092"}}
```

Kafka PLAIN 在 connect 中显式补充 `securityProtocol: "SASL_PLAINTEXT"`、`username`、`password`。不要把真实凭据保存到本文、README 或提交到 Git。

激活成功后刷新 `tools/list`，不能只根据 connect 返回成功就认定 Agent 已发现后续工具。基础观察、取样、投递和消费示例见 [README.md](README.md)；操作约定见 [AGENT_GUIDE.md](AGENT_GUIDE.md)。

新 Agent 的最终交付应写清：

- 实际测试的 Broker、客户端、JDK 和操作系统版本。
- 测试的拓扑及资源范围、认证方式；不得包含明文密码。
- 已通过、失败、未执行的场景，以及可重复的测试入口。
- 修复的文件和问题，离线回归及真实 Broker 验收结果。
- 是否已提交/打包/发布，仍有哪些已知限制。

完成标准是“实现可运行、关键真实消息语义经过验收、失败结果可信、离线回归不退化”，不是只看到 connect 成功或工具列表出现。
