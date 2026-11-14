# Agent Guide

## Activate a connection

1. Call `connector_status` to inspect activation in the current process.
2. Ask the operator for missing connection details, then call the typed connection tool. Pass connection secrets only in its designated parameters.
3. Refresh the tool list after successful activation. If the client caches tools, reconnect it.
4. Confirm the intended target before writes. A runtime change affects all clients of this process. It does not affect another HTTP or stdio process and is lost after restart.

The tools never save runtime configuration to disk. Reconfiguration validates a separate candidate first. Failed validation retains the existing target. Operations already running finish on their captured generation. JDBC transactions retain their original pool across target switches.

## Apollo and Nacos

`apollo_read_config` reads the published namespace, while `apollo_read_item` reads the current editing value through Portal OpenAPI. YAML namespace suffixes are appended when omitted. Item tools support properties only. Portal URL, OpenAPI consumer token, environment, and operator are separate from the published-read access key.

`apollo_update_config` updates an existing key to the complete supplied replacement string. For a partial edit such as changing a port, read the item first and preserve the other parts yourself. Updates publish by default; `publish=false` only saves edits. The tool preserves the existing comment, checks other unpublished edits, and verifies the result. Concurrency checks and publication are not atomic.

`nacos_list_namespaces` lists namespace IDs, names, and counts. `nacos_list_and_count_data_ids` lists all data IDs and counts for exactly one namespace and group. Use namespace IDs, not display names; empty string or `public` selects public.

`nacos_publish_config` creates or replaces an entire document without converting it. Read and retain any content that must survive. Use `convert_config` only when conversion is needed, and supply explicit formats if runtime provider defaults differ from the YAML conversion defaults. Properties values remain strings; conflicting paths and lossy conversions are rejected. Equal formats validate and retain original content.

Read back after publication and compare the resulting content/hash. Never blindly retry `WRITE_RESULT_UNKNOWN`; inspect the Nacos document, or both the Apollo editing item and published release. Treat `CONCURRENT_MODIFICATION` and `VERIFICATION_FAILED` as requiring inspection. No automatic write replay is performed.

## JDBC

Supported production targets are MySQL, Oracle, SQL Server, PostgreSQL, and StarRocks. Use each target's native dialect. H2 is a test adapter only.

- `db_list_tables` and `db_describe_table` inspect metadata, comments, keys, and indexes. An optional schema scopes metadata; it does not change SQL name resolution.
- `db_query` accepts a single read-only SELECT or supported ordinary EXPLAIN. Bind values through `?` parameters. It rejects writes, locking clauses, SELECT INTO, and known side-effect functions. Database permissions remain authoritative.
- `db_execute_dml` accepts one parameterized DML statement. RETURNING and OUTPUT are rejected; generated keys are returned only where the existing driver contract supports them.
- `db_execute_ddl` accepts one supported DDL statement when `omni.jdbc.write.allow-ddl` is true. DROP and TRUNCATE may be irreversible.
- `db_switch_datasource` validates and replaces the global JDBC target; it can also activate initially disabled JDBC. Failed candidates never replace the current pool. Existing work and explicit transactions stay pinned to the original target. Restart restores YAML.

### Explicit transactions

Call `db_execute` with `sql="BEGIN"` and no transaction ID. Retain the returned `transactionId`, then provide it for SQL and the final standalone COMMIT or ROLLBACK. Handles belong to their creating MCP session. Only `sql` and `transactionId` are agent inputs; session identity is supplied by the SDK. SQL scripts use literals rather than a parameter array.

Transaction scripts allow supported queries, estimated plans, DML, and TRUNCATE; other DDL, CALL, dynamic SQL, and session-control commands are rejected. An execution error marks the transaction ROLLBACK_ONLY; only ROLLBACK is then accepted. Validation failures execute nothing and leave the handle usable. There is no automatic commit or retry.

MySQL/Oracle TRUNCATE may implicitly commit previous work and cannot be undone. Observe `implicitCommitPossible` and `rollbackScope`; later rollback covers only work after the implicit commit. Prefer DELETE when full rollback is required. StarRocks BEGIN is rejected.

Keep transactions short: active, idle, lifetime, statement, and result limits apply. Queries see their transaction's own writes. Session deletion, expiry, and graceful shutdown attempt rollback. Disconnecting an SSE stream does not terminate a transaction. Crashes cannot provide a cleanup acknowledgement. A commit/rollback acknowledgement failure produces UNKNOWN state and invalidates the handle; inspect database state independently before deciding what to do. Never blindly retry a lost BEGIN, write, or commit response.

## Redis

`redis_scan_keys` uses SCAN, never KEYS. Continue with the returned cursor and keep the selected database stable during pagination. `redis_inspect_key` returns existence, type, TTL, and size. `redis_read_key` reads bounded string/hash/list/set/zset data.

`redis_write_key` writes supported types, appends lists, and merges hashes. Optional `ttlSeconds` sets expiry. `redis_delete_keys` and `redis_expire_key` are bounded write operations; zero TTL removes expiry. `redis_select_db` validates and changes the logical database for all Redis clients in this process. It is independent of JDBC switching.

`redis_connect` replaces the Redis host/credentials/database generation. Failed PING keeps the old generation. Reconfiguration never inherits credentials from the previous server. Reads and writes respect YAML item/value limits.


## RabbitMQ and Kafka

Activate using `rabbitmq_connect` / `kafka_connect`, then refresh tools. RabbitMQ AMQP
is optional; management-only activation cannot publish or ACK-consume. Connections and
vhosts are global to the process. Do not inherit credentials across targets.

Inspect queue Ready/Unack and rates, then consumers and channel connection names.
Channel Unack may cover multiple queues. Management sampling fetches Ready messages
and requeues them: it changes delivery state and cannot expose already Unack bodies.
For Kafka inspect group assignments and per-partition offsets. Lag is not a business
completion guarantee. `kafka_read_records` does not join or commit to a business group.

Publish tools inject test events into existing topology. Consumer tools stand in for
message receipt, not business processing: RabbitMQ ACKs remove messages; Kafka group
consumption can rebalance business clients and explicit commit=true advances that group.
Use explicit targets and inspect progress/results. Never blindly retry unknown publish,
consume or commit outcomes. Kafka producer idempotence does not deduplicate repeated calls.
Use messageId/correlationId/trace headers and Kafka offsets to correlate application logs.
Real broker acceptance is pending; offline tests do not establish live compatibility.
