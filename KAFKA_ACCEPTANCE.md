# Kafka 真实 Broker 验收记录

日期：2026-11-14（Asia/Shanghai）。分支：`dev_mq`。本轮新增测试和文档，未修改 Kafka 功能代码，尚未提交或发布。

## 结论与环境

**`localhost:9092` 的无认证连接及 8 个 Kafka MCP 工具的核心真实链路通过。** 验收包含 HTTP MCP 双会话、stdio 发行 JAR、YAML 启动激活和实际消息/位点交叉检查。结论限定于本次可访问的单 Broker 环境。

- Bootstrap：`localhost:9092`，`PLAINTEXT`，无认证；元数据返回节点 `localhost:9092`、broker id `1`。
- Kafka Java Client：`4.2.1`；JDK：Oracle `25.0.1`；Maven：`3.9.11`；Windows 10 amd64。
- **本轮未取得 Broker 的具体发行版本。客户端日志中的 `Kafka version: 4.2.1` 是客户端版本，不能据此填写服务端版本。** 不作 Kafka 3.9/4.x 全版本兼容性结论。
- 每次运行生成 `omni-mcp-acceptance-<UUID>` 前缀，创建 5 个独立 Topic（主 Topic 两分区，其余单分区，副本数均为 1）和 5 个独立消费组。
- 测试中的消息投递、消费、提交、删除保留区间和修改组位点全部限定于本次创建的测试资源。没有使用业务消费组或业务 Topic。
- finally 按本次运行的精确名称删除测试消费组和 Topic，并查询确认全部不存在；未修改 Broker 配置，也未删除内部 Topic。

## 已通过场景

`KafkaBrokerTest` 是一个 opt-in 场景测试，输出 `KAFKA_ACCEPTANCE PASS` 检查点，覆盖：

| 范围 | 实际断言 |
| --- | --- |
| 连接、工具与会话 | 无认证激活；工具数 8 → 15；另一个 HTTP MCP 会话可调用业务工具；连接不存在的端口返回 TIMEOUT，旧目标仍可查询 |
| Topic | 名称筛选、数量限制与 truncated；两分区、ISR、末端 offset；空 Topic 读取为空 |
| 投递 | 显式分区、key、trace header，CONFIRMED；按返回的 partition/offset 再读，正文、key、header、timestamp 一致 |
| 观察读取 | earliest/latest/offset/timestamp；gzip 压缩消息；重复 header 和二进制 header 保留；越界 offset 拒绝；不改变已有组提交位点 |
| 不自动创建 | 向不存在 Topic 投递返回 RESOURCE_NOT_FOUND，独立 Admin 检查 Topic 未被创建 |
| 默认消费 | 未指定 commit 时 NOT_REQUESTED；独立 Admin 检查提交位点仍为空；下次从 offset 0 再读 |
| 显式提交 | commit=true 返回 COMMITTED；独立 Admin 检查提交值为实际返回 offset + 1；下一次从该位点续读 |
| 响应限额 | 单次 poll 可取多条，第二条的大 headers 触发响应限制；只返回第一条且只提交到 1；独立观察客户端仍能读取 offset 1；观察读取返回 nextOffset=1 和 truncated=true |
| 消费组与 lag | 列表筛选；无成员组返回已提交位点和正确 lag；真实活跃成员、两分区 assignment；未提交的 lag 为 null、状态 NOT_COMMITTED |
| Rebalance | 一个持续 poll 的真实 fixture 消费者已在组中时，MCP 消费者加入同一测试组，完成重平衡并读到记录，未提交位点 |
| 保留边界 | 在独立测试 Topic 删除 offset 2 之前的区间；起始位点变为 2；提交位点 1 被标记 OUT_OF_RANGE，lag 保持 null；读取 offset 0 被拒绝 |
| 生命周期 | HTTP 客户端和应用关闭；YAML 启动即激活并注册 15 个工具 |

`tests/test_kafka_stdio.py` 由 Java 场景提供测试 Topic/消费组并执行：stdio 初始化、connect、刷新工具、投递/读回、默认不提交、显式提交、EOF 退出。Java 随后通过独立 Admin 确认 stdio 的提交位点，再清理资源。脚本不创建资源；单独运行前必须明确提供符合前缀约束的测试资源，结束后由调用方清理。

## 可重复执行

PowerShell 中指定 JDK 25，并显式允许创建/删除测试资源：

```powershell
$env:JAVA_HOME='C:\Program Files\Java\jdk-25.0.1'
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
$env:OMNI_KAFKA_ACCEPTANCE='true'
$env:KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
mvn --batch-mode --no-transfer-progress -pl omni-connect-app -am test '-DskipTests=false' '-Dtest=KafkaBrokerTest' '-Dsurefire.failIfNoSpecifiedTests=false'
```

需要 Python 3，以及可创建/删除 Topic、读写测试消息、管理测试组位点的权限。测试使用已经存在的发行 JAR 执行 stdio；如功能代码有变更，先执行 `mvn package` 生成当前 JAR，再运行上述验收。业务 MCP 工具本身没有新增创建/删除 Topic、重置组位点能力，资源操作仅由测试 fixture 完成。

默认测试仍完全离线；运行全量离线回归时清除真实测试开关：

```powershell
$env:OMNI_KAFKA_ACCEPTANCE=$null
$env:OMNI_RABBITMQ_ACCEPTANCE=$null
mvn --batch-mode --no-transfer-progress verify '-DskipTests=false'
```

本机沿用 RabbitMQ 验收时的临时 Maven 设置，在上述命令添加 `-o -gs target/rabbitmq-maven-settings.xml -s target/rabbitmq-maven-settings.xml`；未修改全局 Maven/JDK 配置。

## 证据与未覆盖范围

- `target/kafka-live.log`：真实场景测试通过，1 项、0 失败；包含 stdio 位点交叉验证和资源清理结果。修正子进程输出捕获后再次执行通过，无 Surefire 通信警告。
- `target/kafka-stdio.log`：stdio 场景检查点；子进程日志使用文件捕获，避免影响 Surefire 通信。
- `target/kafka-regression.log`：全量 `verify` 成功，298 项、295 通过、0 失败/错误、3 跳过（含默认禁用的 RabbitMQ 和 Kafka 真实 Broker 测试）。
- `target/kafka-release-tests.log`：当前发行 JAR 烟测通过，包含 HTTP 鉴权、启动/重启/停止、三个 stdio 子进程、EOF、进程隔离和发行布局。使用允许读取 Windows CIM 的执行环境运行。
- `target/release/omni-connect-mcp/omni-connect-mcp.jar`：可执行 JAR，版本仍为 `0.1.1`。

全部验收结束后，另启动一个独立 stdio 进程查询，确认 `omni-mcp-acceptance-` 前缀的 Topic 和消费组数量均为 0。

未执行：SASL/PLAIN 凭据和 ACL 矩阵、多 Broker 故障切换、断网或 Broker 重启、真实提交失败/结果未知、poll 中途异常的部分结果、操作期间切换、长期线程泄漏、压缩 Topic 清理产生的 offset 间隙、自动消费 reset=latest/none、多版本兼容性。没有为验收重启本机 Kafka 或修改其配置。

交接文档中的 P1 边界仍保留：严格端到端总耗时、外层元数据/headers 的整体响应限额、消费响应的截断提示、poll 中途异常保留已读结果。当前验收不能证明这些边界已经解决。lag 和 CONFIRMED 均不代表应用业务处理完成。
