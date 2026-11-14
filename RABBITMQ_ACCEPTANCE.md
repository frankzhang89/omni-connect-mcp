# RabbitMQ 真实 Broker 验收记录

日期：2026-11-14（Asia/Shanghai）。分支：`dev_mq`。本轮修改尚未提交或发布。

## 结论与范围

**提供的 Broker 上，9 个 RabbitMQ 工具的核心消息链路验收通过。** HTTP MCP、stdio、YAML 启动激活和离线回归通过。此结论限定于实际环境，不代表故障注入矩阵、其他 RabbitMQ 版本或 Kafka 已验收。

- Broker：`10.246.183.9:5672`，Management HTTP：同主机 `15672`。
- RabbitMQ / Management：`3.6.5`；Erlang：`18.3`。
- RabbitMQ Java Client：`5.30.0`；JDK：Oracle `25.0.1`；Maven：`3.9.11`；Windows 10 amd64。
- 用户授权创建并删除 `omni-mcp-acceptance` 前缀的独立测试 vhost；每次运行生成 UUID，仅在该 vhost 创建测试队列、交换机和权限，不投递、取样或消费业务队列。
- HTTP 和 stdio 测试均在 finally 删除测试 vhost。验收后额外查询确认此前缀 vhost 列表为空。
- Python 初次管理请求被本机代理影响；直连成功。Java 管理请求及 AMQP 认证成功。

## 已通过

`RabbitMqBrokerTest` 是一个场景测试，包含以下断言，并输出 `RABBITMQ_ACCEPTANCE PASS` 检查点：

- 管理接口认证、AMQP 认证；管理接口单独激活时，投递返回 `CAPABILITY_NOT_CONFIGURED`。
- 从 8 个基础工具刷新到 16 个工具；两个 HTTP MCP 会话共享激活结果。
- 错误管理密码返回 `AUTHENTICATION_FAILED`，失败替换保留原有可用目标。
- 队列名包含空格和 `/` 时，查询、取样及消费正确；按名称筛选和限制页大小。
- 默认交换机和 direct 交换机可路由投递返回 `CONFIRMED`；不可路由返回 `UNROUTABLE`；不存在交换机/队列返回 `RESOURCE_NOT_FOUND`。
- 队列/交换机/默认交换机绑定查询。
- 取样返回正确正文，并重新入队；后续仍可消费；512 字节消息的 64 字节预览标记截断且保留原始大小。
- 确认消费返回 `ackStatus=SENT` 和正确计数；messageId、correlationId、trace header 保留；独立 AMQP 客户端检查消息已移除。
- 真实消费者 prefetch=1 持有 Unack；队列、消费者、Channel 和客户端连接名关联正确；取样不能读出 Unack；消费者 Channel 关闭后消息重新可消费。
- headers 使响应超限时，消息不 ACK，仍可由独立客户端取回。
- 连接和应用正常关闭；YAML 启动激活后可查询真实队列。

`tests/test_rabbitmq_stdio.py` 使用发行 JAR，验证 stdio 初始化、connect、16 个工具刷新、confirm 投递、取样重新入队、ACK 消费和 EOF 退出；日志在独立测试目录，不污染发行布局。

## 修复

1. 取样请求同时发送 `ackmode=ack_requeue_true` 和旧版所需的 `requeue=true`，直接兼容，不重试可能有副作用的请求。[3.6.5 接口源码](https://github.com/rabbitmq/rabbitmq-management/blob/rabbitmq_v3_6_5/src/rabbit_mgmt_wm_queue_get.erl)。
2. 管理接口查询默认交换机绑定时，将空名称映射为 `amq.default`。[3.6.5 路径解析源码](https://github.com/rabbitmq/rabbitmq-management/blob/rabbitmq_v3_6_5/src/rabbit_mgmt_util.erl)。
3. 移除队列分页请求中的 `columns`，收到分页结果后本地裁剪字段。实测旧版对分页外层再次裁剪，原请求把非空队列列表返回成 `[]`。保留原有 2 MiB 管理响应限制，大队列页可能需要减小 pageSize。

上述修复已加入离线回归断言。

## 可重复执行

先在当前进程设置 JDK 25 的 `JAVA_HOME` 和 PATH，并通过环境变量提供：

```text
OMNI_RABBITMQ_ACCEPTANCE=true
RABBITMQ_MANAGEMENT_URL=http://<host>:15672
RABBITMQ_AMQP_ADDRESSES=<host>:5672
RABBITMQ_USERNAME=<test-admin>
RABBITMQ_PASSWORD=<secret>
```

只有在允许创建/删除独立测试 vhost 的环境才能启用；需要管理 vhost 和权限的能力。不要把凭据提交到 Git。默认构建不连接真实 Broker。

```text
mvn --batch-mode --no-transfer-progress -pl omni-connect-app -am test -DskipTests=false -Dtest=RabbitMqBrokerTest -Dsurefire.failIfNoSpecifiedTests=false
mvn --batch-mode --no-transfer-progress verify -DskipTests=false
python tests/test_rabbitmq_stdio.py
python tests/test_release.py
```

离线全量回归运行时取消设置 `OMNI_RABBITMQ_ACCEPTANCE`。本机原 Maven 全局配置包含缓慢的内部仓库，本轮使用 `target/rabbitmq-maven-settings.xml` 作为临时 `-gs` / `-s` 设置、保留原本地缓存；未修改全局 Maven/JDK 配置。补齐依赖后使用 `-o` 执行。Windows 发布烟测需要 CIM 进程查询权限。

## 证据及剩余范围

- `target/rabbitmq-live.log`：HTTP MCP 真实场景通过，1 项、0 失败。
- `target/rabbitmq-stdio.log`：stdio 真实场景通过。
- `target/rabbitmq-regression.log`：全量 `verify` 成功，Surefire 合计 297 项、295 通过、0 失败/错误、2 跳过（含默认禁用的真实 Broker 测试）。
- `target/rabbitmq-release-tests.log`：发行烟测通过，包含 HTTP 鉴权、启动/重启/停止、三个 stdio 子进程、EOF、隔离和发行布局。
- 可执行 JAR：`target/release/omni-connect-mcp/omni-connect-mcp.jar`，版本仍为 `0.1.1`。

未执行：RabbitMQ 4.x 兼容性、受限账号/vhost 权限矩阵、反向代理前缀、大 vhost、confirm nack/超时/断网、ACK 途中连接失败、操作期间切换与线程泄漏专项测试。没有对正式 Broker 做重启或网络故障注入。已有离线替身测试不能替代上述真实验证。

原交接文档中的管理连接超时、严格总耗时预算、整体响应大小及截断标志等 P1 边界仍保留；本轮不承诺自动重连、严格端到端 30 秒或业务处理完成。后续同日完成的 Kafka 验收另见 [KAFKA_ACCEPTANCE.md](KAFKA_ACCEPTANCE.md)。
