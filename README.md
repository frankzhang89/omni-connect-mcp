# Omni Connect MCP

One MCP server for Apollo, Nacos, relational databases, standalone Redis, RabbitMQ, and Kafka. It runs from one executable JAR with one external `application.yml` and supports both stdio and Streamable HTTP.

See [Release Notes](RELEASE_NOTES.md) for version history. Every code change must update
the English release notes in the same change; record post-release changes under `Unreleased`.

## Capabilities

| Connector | Capabilities |
| --- | --- |
| Apollo | Read published configurations; read and update existing properties items through Portal OpenAPI; optionally publish updates. |
| Nacos | Discover namespaces and configuration IDs; read, create, and replace complete configuration documents. Detects Nacos 1.x/2.x/3.x once per connection and selects version-specific APIs. |
| JDBC | Connect to MySQL, PostgreSQL, Oracle, SQL Server, and StarRocks; inspect metadata; query data; execute DML and DDL; manage explicit transactions and switch databases at runtime. |
| Redis | Connect to standalone Redis; scan keys; inspect type, TTL, and size; read and write string, list, set, hash, and sorted-set values; manage expiry and logical database selection. |
| RabbitMQ | Inspect queues, bindings, consumers and Channel Unack; sample Ready messages; publish and ACK-consume bounded messages. |
| Kafka | Inspect topics, partitions, consumer groups and lag; observe records without commits; publish and simulate bounded group consumption. |
| Configuration conversion | Convert properties and YAML offline, without connecting to a configuration server. |

## Quick start

Java 25+ is required. Download the Windows ZIP or Linux tar.gz from the [Releases page](https://github.com/frankzhang89/omni-connect-mcp/releases). Extract the platform archive and keep the JAR, example YAML, and launchers together:

```text
omni-connect-mcp/
  omni-connect-mcp.jar
  application.yml
  <platform launchers>
```

The `Source code` downloads contain source files, not the runnable application. A JDK is not bundled with either platform package.

### Streamable HTTP

Edit `application.yml`, then set the HTTP Bearer token and start the server.

Windows PowerShell:

```powershell
$env:OMNI_AUTH_TOKEN = '<your-shared-token>'
.\omni-connect.cmd start
.\omni-connect.cmd stop
.\omni-connect.cmd restart --server.port=8286
```

Linux:

```bash
export OMNI_AUTH_TOKEN='<your-shared-token>'
bash ./omni-connect.sh start
bash ./omni-connect.sh stop
bash ./omni-connect.sh restart --server.port=8286
```

Configure your MCP client with:

| Client setting | Value |
| --- | --- |
| Transport | Streamable HTTP |
| URL | `http://localhost:8285/mcp` |
| Header | `Authorization: Bearer <your-shared-token>` |

The URL changes when `server.port` or `spring.ai.mcp.server.streamable-http.mcp-endpoint` changes. HTTP launchers force HTTP mode. Linux launchers require Bash 4.4+, `flock`, `setsid`, `readlink`, and `ss`.

On Windows, double-click `omni-connect.cmd` for the command menu. Up/Down or 1/2/3 select Start/Stop/Restart; Enter executes the selection. Restart is selected initially. Five seconds without keyboard input executes the selected command.

To select a JDK, set `OMNI_JAVA_HOME` or `JAVA_HOME`, or pass an absolute JDK directory or environment-variable name to the HTTP launcher:

```powershell
.\omni-connect.cmd start --java-home=JDK25
.\omni-connect.cmd restart "--java-home=C:\Program Files\Java\jdk-25.0.1"
```

### Stdio

Let the MCP client launch and own the process. For clients that accept a `mcpServers` JSON configuration, use an entry such as:

```json
{
  "mcpServers": {
    "omni_connect_stdio": {
      "command": "C:/Program Files/Java/jdk-25.0.1/bin/java.exe",
      "args": [
        "-jar", "E:/deploy/omni-connect-mcp/omni-connect-mcp.jar",
        "--spring.config.location=file:E:/deploy/omni-connect-mcp/application.yml",
        "--spring.ai.mcp.server.stdio=true"
      ]
    }
  }
}
```

Client configuration formats vary; the executable and arguments above can also be entered in clients with dedicated stdio settings. On Linux, use the installed Java executable and Linux deployment paths. Optional stdio wrappers are `omni-connect-stdio.cmd` on Windows and `omni-connect-stdio` on Linux. They load the YAML beside the JAR.

Stdio opens no HTTP port and uses no HTTP Bearer token. Standard output carries MCP messages only; diagnostics go to stderr and PID-specific files under `log/`. Closing stdin terminates the process.

An HTTP service and multiple stdio processes can run simultaneously. Each process has independent connection state; all sessions within one process share its current connections. HTTP launchers do not stop client-owned stdio processes.

## Configure application.yml

The YAML is external and is not embedded in the JAR. The launchers load the adjacent file; when launching Java directly, specify `--spring.config.location=file:/absolute/path/application.yml`.

The following is the complete example configuration shipped with this project. `${VARIABLE:default}` reads an environment variable and uses the value after `:` when it is absent. Supply real connection values through environment variables or your deployment's YAML.

```yaml
# The only deployment configuration; runtime connections never modify this file.
spring:
  application:
    name: omni-connect-mcp
  ai:
    mcp:
      server:
        name: omni-connect-mcp
        version: 0.2.0
        stdio: false
        protocol: STREAMABLE
        type: SYNC
        annotation-scanner:
          enabled: false
        capabilities:
          tool: true
          resource: false
          prompt: false
          completion: false
        streamable-http:
          mcp-endpoint: /mcp
        instructions: >-
          Use connector_status and the connection tools to activate capabilities.
          Connections are shared by this process, memory-only, and reset on restart.
          Refresh tools after activation. Nacos publication replaces the entire document
          without conversion. Apollo item updates publish by default. Read before editing
          and verify writes. Never blindly retry an unknown write outcome. Use the
          connected database dialect. Explicit transactions belong to their MCP session
          and retain their original data source across switches. Read AGENT_GUIDE.md.
server:
  address: ${SERVER_ADDRESS:0.0.0.0}
  port: ${SERVER_PORT:8285}
omni:
  auth:
    enabled: true
    # Set a real token before HTTP startup. Stdio does not use HTTP authentication.
    token: ${OMNI_AUTH_TOKEN:}
  config:
    http:
      connect-timeout: 3s
      read-timeout: 10s
    apollo:
      enabled: false
      meta: ${APOLLO_META:}
      app-id: ${APOLLO_APP_ID:}
      cluster: default
      namespace: application
      file-type: properties
      access-key: ${APOLLO_ACCESS_KEY:}
      portal-url: ${APOLLO_PORTAL_URL:}
      openapi-token: ${APOLLO_OPENAPI_TOKEN:}
      env: ${APOLLO_ENV:}
      operator: ${APOLLO_OPERATOR:}
      released-by: ${APOLLO_RELEASED_BY:}
    nacos:
      enabled: false
      server-addr: ${NACOS_SERVER_ADDR:}
      context-path: /nacos
      username: ${NACOS_USERNAME:}
      password: ${NACOS_PASSWORD:}
      namespace: ""
      group: DEFAULT_GROUP
      file-type: yaml
  jdbc:
    enabled: false
    connection:
      url: ${JDBC_URL:}
      username: ${JDBC_USERNAME:}
      password: ${JDBC_PASSWORD:}
      max-active: 3
      min-idle: 0
      connection-timeout: 5000
    switch:
      timeout-seconds: 15
      validation-timeout-seconds: 5
    query:
      max-rows: 100
      timeout-seconds: 10
      max-cell-chars: 4000
    write:
      allow-ddl: true
    transaction:
      idle-timeout-seconds: 120
      max-lifetime-seconds: 600
      max-active: 5
      max-statements: 20
      max-sql-chars: 20000
      script-timeout-seconds: 30
  redis:
    enabled: false
    connection:
      host: ${REDIS_HOST:localhost}
      port: ${REDIS_PORT:6379}
      username: ${REDIS_USERNAME:}
      password: ${REDIS_PASSWORD:}
      database: 0
    limits:
      max-databases: 16
      max-scan-count: 100
      max-items: 100
      max-value-chars: 4000
```

### Connection and execution settings

| YAML section | Configuration |
| --- | --- |
| `omni.auth` | HTTP authentication. `enabled: true` requires a nonempty `token`; clients send it as a Bearer token. Stdio does not require it. |
| `omni.config.http` | Apollo/Nacos connection and read timeouts, expressed as durations such as `3s`. |
| `omni.config.apollo` | Set `enabled: true`, `meta`, and `app-id` for startup activation. Published reads use `cluster`, `namespace`, `file-type`, and optional signing `access-key`. Item operations additionally require `portal-url`, `openapi-token`, and `env`; updates require an operator. |
| `omni.config.nacos` | Set `enabled: true` and `server-addr` for startup activation. Use one origin such as `http://nacos.example.com:8848`; set `context-path` separately, normally `/nacos`. Supply credentials when authentication is enabled. `namespace` is an ID; empty string or `public` selects public. `group` and `file-type` provide operation defaults. |
| `omni.jdbc` | Set `enabled: true` and connection URL/credentials for startup activation. The JDBC driver is selected from the URL. `max-active` and `min-idle` control the pool; `connection-timeout` is in milliseconds. |
| `omni.jdbc.query` | Result row limit, query timeout in seconds, and maximum returned characters per cell. |
| `omni.jdbc.switch` | Candidate connection and validation timeouts in seconds. |
| `omni.jdbc.write` | `allow-ddl` enables or blocks DDL. The shipped value is `true`; set it to `false` to block the DDL tool. DML remains available subject to database permissions. |
| `omni.jdbc.transaction` | Idle/lifetime/script timeouts in seconds, concurrent transaction limit, and SQL/statement limits. |
| `omni.redis` | Set `enabled: true` with `connection.host`, `port`, optional credentials, and `database` for startup activation. Keep host and port separate. `limits` bounds database indices, scans, returned items, and value length. |

For example, a MySQL URL is `jdbc:mysql://mysql.example.com:3306/example?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC`. Set `JDBC_URL`, `JDBC_USERNAME`, and `JDBC_PASSWORD`, then change `omni.jdbc.enabled` to `true` to connect at startup.

All six connectors are disabled in the example YAML. Their `enabled` flags control startup activation, not whether an agent may connect later. JDBC validates its configured connection during startup; Nacos detects its server version during startup activation; Apollo and Redis initialize without a startup connectivity probe. Runtime connection tools validate their candidate before replacing the active connection.

## Connection lifecycle and tool discovery

There are **43 tools** when all connectors are active. With every connector disabled, these eight remain available:

`connector_status`, `apollo_connect`, `nacos_connect`, `db_switch_datasource`, `redis_connect`, `rabbitmq_connect`, and `kafka_connect`, plus the offline `convert_config` tool.

1. Call `connector_status` to inspect active connectors.
2. Activate the required connector using its connection tool and your connection parameters.
3. Have the MCP client refresh `tools/list` to discover the business tools, then call them.

Activation sends tool-list change notifications. Some clients cache the initial tool list and do not expose newly registered tools to the agent automatically. A successful connection or `connector_status` response does not prove that the client refreshed its callable tools. Check the actual `tools/list` response. Reconnecting to an HTTP service preserves the connection if that service stays running; restarting a stdio process resets its state, so runtime activation must be repeated. Clients that only discover tools at startup can use YAML startup activation; runtime registration occurs during application startup, so startup timing should also be considered.

Runtime connection changes are process-wide, memory-only, and never saved to YAML. A process restart restores YAML settings. HTTP and stdio connections do not configure one another. Omitted optional fields use defaults rather than credentials from the previous connection. Failed candidate validation leaves the previous connection available. Runtime disabling is not provided. Status reports activation, not a continuous health check.

## Tool reference

Parameter names in tool calls use camelCase; YAML configuration uses kebab-case. The tables below list required parameters first, followed by optional parameters. The MCP client's discovered input schema is the full parameter contract.

### Shared tools

| Tool | Parameters | Behavior |
| --- | --- | --- |
| `connector_status` | None | Returns activation state, configuration source, and registered tool names for each connector; omits addresses and credentials. |
| `convert_config` | Required: `content`. Optional: `sourceType`, `targetType`. | Offline properties/YAML conversion. Supports `properties`, `yaml`, and `yml`; same-format conversion validates and preserves content. Explicit formats avoid depending on provider defaults. |

### Apollo

| Tool | Parameters | Behavior |
| --- | --- | --- |
| `apollo_connect` | Required: `meta`, `appId`. Optional: `cluster`, `namespace`, `fileType`, `accessKey`, `portalUrl`, `openapiToken`, `env`, `operator`, `releasedBy`. | Validates a published namespace and activates/replaces Apollo. Defaults: cluster `default`, namespace `application`, format `properties`. Portal permissions are checked by item operations. |
| `apollo_read_config` | Optional: `appId`, `cluster`, `namespace`, `fileType`. | Reads published content with location, release key, and SHA-256. Omitted identifiers use active connection defaults. |
| `apollo_read_item` | Required: `key`. Optional: `appId`, `cluster`, `namespace`, `fileType`, `env`. | Reads a properties item's current editing value, comment, and metadata through Portal OpenAPI; it may differ from the published value. |
| `apollo_update_config` | Required: `key`, `value`. Optional: `appId`, `cluster`, `namespace`, `fileType`, `env`, `publish`, `operator`, `releasedBy`, `releaseTitle`, `releaseComment`. | Updates an existing properties item to the complete supplied value and preserves its comment. Publishes by default; `publish: false` only saves the editing value. |

For published YAML reads, missing namespace suffixes are appended according to `fileType`; `.yaml` and `.yml` are significant. Item read/update tools support properties namespaces only. Before updating part of a value, read the full item and preserve everything that should remain. Publication can include a namespace's changes; the tool checks for other unpublished edits, but those checks and publication are not atomic.

### Nacos

| Tool | Parameters | Behavior |
| --- | --- | --- |
| `nacos_connect` | Required: `serverAddr`. Optional: `contextPath`, `username`, `password`, `namespace`, `group`, `fileType`. | Detects and returns the server `version`, validates namespace discovery, and activates/replaces Nacos. Defaults: context `/nacos`, public namespace, group `DEFAULT_GROUP`, format `yaml`. |
| `nacos_list_namespaces` | None | Lists namespace IDs, display names, and configuration counts. |
| `nacos_list_and_count_data_ids` | Optional: `namespace`, `group`. | Lists all data IDs and their total count for exactly one namespace/group, following all pages. |
| `nacos_read_config` | Required: `dataId`. Optional: `group`, `namespace`, `fileType`. | Reads unchanged content and SHA-256. `dataId` is exact: no suffix is inferred or added. |
| `nacos_publish_config` | Required: `dataId`, `content`. Optional: `group`, `namespace`, `fileType`. | Creates or replaces the entire document. Content must already match `fileType`; publication never converts it. |

Each new Nacos connection detects the server version once before activation. Startup activation (`omni.config.nacos.enabled: true` with `server-addr`) detects immediately; when disabled, no Nacos request is sent until `nacos_connect`. Detection requests `{serverAddr}{contextPath}/v3/admin/core/state`, trying `/v1/console/server/state` only if the V3 state endpoint returns HTTP 404 or 410. It accepts a top-level `version` or a successful envelope containing `data.version`. Missing or malformed versions, unsupported major versions, authentication errors, and network failures stop activation. A failed runtime activation retains the previous connection and version.

| Server version | Namespace list | Configuration read / publish | Configuration list | Login |
| --- | --- | --- | --- | --- |
| 1.x, 2.0.x, 2.1.x | `/v1/console/namespaces` | `/v1/cs/configs` | `/v1/cs/configs` | `/v1/auth/login` |
| 2.2.x and later 2.x | `/v2/console/namespace/list` | `/v1/cs/configs` | `/v1/cs/configs` | `/v1/auth/login` |
| 3.x | `/v3/admin/core/namespace/list` | `/v3/admin/cs/config` | `/v3/admin/cs/config/list` | `/v3/auth/user/login` |

All paths use the configured server origin and context path. Nacos 3.x uses server Admin APIs, so no separate console URL is needed. Supply an account with namespace-list/Admin API access and the required configuration read/write permissions when authentication is enabled. The connector sends 3.x tokens in the `accessToken` header; earlier versions use query/form parameters. Public namespace input remains empty string or `public`, and namespace-list output normalizes the public ID to empty string.

The immutable detected version is returned by `nacos_connect` and `connector_status.nacos.version`. Requests and MCP sessions in one process reuse that connection's API selection; business endpoint failures never trigger version probing or API downgrade. Read authentication may refresh a token and retry once on the same API; publications are never automatically replayed. Reconnect with `nacos_connect` after a server upgrade. Restarting the process discards the cached version and runtime settings.

Compatibility tests model representative 1.4.x, 2.0/2.1, 2.2, 2.4, 3.0 and 3.2 contracts with local HTTP peers and a Postman simulation. These are not an exhaustive live-server certification of every patch release.

### JDBC

| Tool | Parameters | Behavior |
| --- | --- | --- |
| `db_switch_datasource` | Required: `jdbcUrl`, `username`, `password`. Optional: `schema`. | Validates a candidate JDBC connection and switches the process-wide target. Also activates JDBC when initially disabled. |
| `db_list_tables` | None | Lists up to 500 tables/views with available comments and catalog/schema information. |
| `db_describe_table` | Required: `tableName`. Optional: `schemaName`. | Returns columns, types, defaults, comments, primary keys, indexes, foreign keys, and unique constraints where supported. |
| `db_query` | Required: `sql`. Optional: `parameters`, `maxRows`. | Runs one read-only SELECT or WITH SELECT. Supports ordinary EXPLAIN SELECT on MySQL, PostgreSQL, and StarRocks. |
| `db_execute_dml` | Required: `sql`. Optional: `parameters`. | Executes one INSERT, UPDATE, DELETE, MERGE, or UPSERT supported by the database; returns affected rows and supported generated keys. Executes independently of explicit transaction handles. |
| `db_execute_ddl` | Required: `sql`. | Executes one CREATE, ALTER, DROP, TRUNCATE, or RENAME statement when DDL is enabled. |
| `db_execute` | Required: `sql`. Optional: `transactionId`. | Opens an explicit transaction with `BEGIN`, executes SQL on its retained connection, and closes it with `COMMIT` or `ROLLBACK`. |

Use the connected database's SQL dialect. `db_query` and `db_execute_dml` bind values with `?` placeholders and `parameters`. Neither RETURNING nor OUTPUT is supported by the DML tool. `db_query` rejects writes, locking clauses, EXPLAIN ANALYZE, and EXPLAIN options. Oracle/SQL Server estimated plans use `db_execute` instead.

Explicit transactions belong to their MCP session and require `db_execute` for every statement; the other query/DML tools do not participate in them. Pass the ID returned by `BEGIN` on subsequent calls. Existing transactions retain their original data source across switches. Execution errors require rollback. StarRocks does not support opening these explicit transactions. Transaction scripts allow SELECT, supported EXPLAIN, DML, and TRUNCATE; other DDL and session-control statements are rejected. MySQL/Oracle TRUNCATE can implicitly commit and cannot be treated as rollback-safe.

### Redis

| Tool | Parameters | Behavior |
| --- | --- | --- |
| `redis_connect` | Required: `host`. Optional: `port`, `username`, `password`, `database`. | Uses PING to validate and activate/replace a standalone Redis connection. Defaults: port `6379`, database `0`. |
| `redis_scan_keys` | Optional: `pattern`, `cursor`, `count`. | Uses SCAN to page through matching keys. Start with cursor `"0"` and continue using `nextCursor`. |
| `redis_inspect_key` | Required: `key`. | Returns existence, type, TTL, and size without changing data. |
| `redis_read_key` | Required: `key`. Optional: `maxItems`. | Reads bounded string/list/set/hash/zset content. |
| `redis_write_key` | Required: `key`, `type`, and the value field appropriate for the type. Optional: `ttlSeconds`. | Writes one key. Use `value` for string; `values` for list/set; `fields` for hash; `scoreMembers` for zset. |
| `redis_delete_keys` | Required: `keys`. | Deletes up to 100 supplied keys and returns the removed count. |
| `redis_expire_key` | Required: `key`, `ttlSeconds`. | Sets expiry; `0` removes expiry with PERSIST rather than deleting the key. |
| `redis_select_db` | Required: `db`. | Changes the Redis logical database for all sessions in this process. |

Lists append to the tail, sets add members, hashes merge fields, and sorted sets add/update member scores. Supply a positive `ttlSeconds` to `redis_write_key` to set expiry. Keep the logical database stable during a multi-page scan. Redis Cluster and Sentinel connection modes are not provided.

## Calling tools

The examples below are the `params` object of an MCP `tools/call` request. Pass `name` and `arguments` through your MCP client after initialization; these are not REST endpoints. For example, a full protocol request looks like:

```json
{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"connector_status","arguments":{}}}
```

### Apollo: connect, then read

```json
{"name":"apollo_connect","arguments":{"meta":"http://apollo.example.com:8080","appId":"example-app","cluster":"default","namespace":"application","fileType":"properties"}}
```

```json
{"name":"apollo_read_config","arguments":{}}
```

For item operations, include Portal settings in `apollo_connect`: `portalUrl`, `openapiToken`, `env`, and `operator`, plus `releasedBy` when a separate publisher is required. Then read the existing item before changing it:

```json
{"name":"apollo_read_item","arguments":{"key":"service.url"}}
```

```json
{"name":"apollo_update_config","arguments":{"key":"service.url","value":"https://api.example.com/v2","publish":false}}
```

### Nacos: discover, read, then publish

```json
{"name":"nacos_connect","arguments":{"serverAddr":"http://nacos.example.com:8848","username":"example-user","password":"<password>","namespace":"example-namespace-id","group":"EXAMPLE_GROUP","fileType":"yaml"}}
```

```json
{"name":"nacos_list_namespaces","arguments":{}}
```

```json
{"name":"nacos_list_and_count_data_ids","arguments":{"namespace":"example-namespace-id","group":"EXAMPLE_GROUP"}}
```

```json
{"name":"nacos_read_config","arguments":{"dataId":"example.yaml"}}
```

Publish only after reviewing the complete replacement document, then read it back:

```json
{"name":"nacos_publish_config","arguments":{"dataId":"example.yaml","content":"app:\n  name: example\n","fileType":"yaml"}}
```

To convert formats explicitly before publishing:

```json
{"name":"convert_config","arguments":{"content":"app.name=example\n","sourceType":"properties","targetType":"yaml"}}
```

### JDBC: connect and query

```json
{"name":"db_switch_datasource","arguments":{"jdbcUrl":"jdbc:mysql://mysql.example.com:3306/example?serverTimezone=UTC","username":"example-user","password":"<password>"}}
```

```json
{"name":"db_list_tables","arguments":{}}
```

```json
{"name":"db_describe_table","arguments":{"tableName":"example_items"}}
```

```json
{"name":"db_query","arguments":{"sql":"SELECT id, name FROM example_items WHERE id = ?","parameters":[1],"maxRows":10}}
```

The following DML assumes the example table already exists and independently applies the change:

```json
{"name":"db_execute_dml","arguments":{"sql":"UPDATE example_items SET name = ? WHERE id = ?","parameters":["updated",1]}}
```

To control the transaction instead, use the same MCP session for all these calls. Replace `<transaction-id>` with the ID returned by `BEGIN`; `db_execute` accepts SQL literals, not a `parameters` array:

```json
{"name":"db_execute","arguments":{"sql":"BEGIN"}}
```

```json
{"name":"db_execute","arguments":{"sql":"UPDATE example_items SET name = 'temporary' WHERE id = 1","transactionId":"<transaction-id>"}}
```

```json
{"name":"db_execute","arguments":{"sql":"ROLLBACK","transactionId":"<transaction-id>"}}
```

Use `COMMIT` instead of `ROLLBACK` when the changes should be saved. DDL is a separate operation, for example:

```json
{"name":"db_execute_ddl","arguments":{"sql":"CREATE TABLE example_items (id BIGINT PRIMARY KEY, name VARCHAR(100))"}}
```

### Redis: connect and write each supported type

```json
{"name":"redis_connect","arguments":{"host":"redis.example.com","port":6379,"password":"<password>","database":0}}
```

Each call below writes one example key with a 300-second TTL:

```json
{"name":"redis_write_key","arguments":{"key":"example:string","type":"string","value":"hello","ttlSeconds":300}}
```

```json
{"name":"redis_write_key","arguments":{"key":"example:list","type":"list","values":["hello"],"ttlSeconds":300}}
```

```json
{"name":"redis_write_key","arguments":{"key":"example:set","type":"set","values":["hello"],"ttlSeconds":300}}
```

```json
{"name":"redis_write_key","arguments":{"key":"example:hash","type":"hash","fields":{"message":"hello"},"ttlSeconds":300}}
```

```json
{"name":"redis_write_key","arguments":{"key":"example:zset","type":"zset","scoreMembers":{"hello":1.5},"ttlSeconds":300}}
```

Read back a value, check its TTL, or scan the example keys:

```json
{"name":"redis_read_key","arguments":{"key":"example:hash"}}
```

```json
{"name":"redis_inspect_key","arguments":{"key":"example:hash"}}
```

```json
{"name":"redis_scan_keys","arguments":{"pattern":"example:*","cursor":"0","count":100}}
```

## Errors and write verification

Check tool results for `isError`, `success`, and any returned error code before continuing. Connection success does not prove that every operation is authorized by the upstream service. Read configurations before replacing them and read back after publication to verify the returned content/hash. On `WRITE_RESULT_UNKNOWN`, inspect the target before retrying: the write may already have completed. On an uncertain transaction commit/rollback, inspect database state rather than replaying the write.

See [AGENT_GUIDE.md](AGENT_GUIDE.md) for detailed transaction, publication, and recovery rules.

## Build from source

Select JDK 25+ and run Maven 3.8+ recursively from the project root:

```powershell
mvn clean package
mvn clean package '-Dbase.export.path={yourExpectedPath}'
```

The default export is `target/release/omni-connect-mcp/omni-connect-mcp.jar`. Replace `{yourExpectedPath}` with your desired absolute output directory; the second command exports the JAR to `{yourExpectedPath}/omni-connect-mcp/omni-connect-mcp.jar`. Maven exports only the runnable JAR. Copy the example YAML and required platform launchers separately when assembling a deployment from a local build. Intermediate artifacts remain in module `target/` directories.

Tests are skipped by default. Run `mvn clean verify -DskipTests=false` to execute them and `python tests/test_release.py` with `JAVA_HOME` set to JDK 25 to exercise the packaged JAR and launchers.

### Publishing a release

Starting with v0.2.0, pushing a version tag such as `v0.2.0` triggers the tagged-build
workflow. Commit the matching Maven/configuration versions and an English
`## v0.2.0` section in [RELEASE_NOTES.md](RELEASE_NOTES.md) before tagging.
The workflow checks that the tag matches the root Maven version, extracts only that
version's Markdown section, runs the build and tests, and publishes the notes as the
GitHub Release body alongside the Windows ZIP and Linux tar.gz files. Missing,
duplicate or empty sections stop the workflow before building or publishing.

The release notes are read from the tagged source. `Unreleased` and other versions
are excluded. Rerunning a tagged build refreshes that release's assets and replaces
its body with the notes from the same tag, overwriting manual edits made on the
GitHub website. Existing historical releases are not backfilled.

Preview the selected notes and run the offline release automation tests with:

```powershell
python scripts/extract_release_notes.py v0.2.0 --output target/release-notes.md
python -m unittest discover -s tests -p test_release_notes.py -v
```

The workflow keeps its prepared notes in the runner's temporary directory so that
`mvn clean` does not remove them. The publishing script requires GitHub CLI and uses
the workflow's existing `GITHUB_TOKEN` through `GH_TOKEN` with `contents: write`.


### RabbitMQ and Kafka (development diagnostics)

The two independent modules add 17 tools. Only `rabbitmq_connect` and `kafka_connect`
are advertised before activation; successful activation registers 8 RabbitMQ / 7 Kafka
business tools. Refresh `tools/list`. All targets are process-wide, memory-only;
failed replacements retain the old target. YAML `enabled` controls startup activation.
With all connectors disabled there are 8 tools; all six enabled expose 43.

RabbitMQ requires an HTTP Management API (including an optional reverse-proxy prefix).
AMQP addresses are optional: management-only targets can inspect and sample, while
publish/consume return `CAPABILITY_NOT_CONFIGURED`. If AMQP addresses are supplied,
both connections must validate. Missing AMQP credentials reuse the credentials from
this request, never from a previous target. Kafka supports `PLAINTEXT` or
`SASL_PLAINTEXT` with PLAIN. TLS, SCRAM, Kerberos and OAuth are outside this version.

| Tool | Arguments and behavior |
|---|---|
| `rabbitmq_connect` | Required `managementUrl`, `username`, `password`; optional `virtualHost` (default `/`), comma-separated `amqpAddresses`, `amqpUsername`, `amqpPassword`. |
| `rabbitmq_list_queues` | Optional `nameContains`, `page`, `pageSize`; queue type, Ready, Unack, consumer counts. |
| `rabbitmq_inspect_queue` | Required `queue`; counts, rates, arguments and effective policies. Missing metrics stay null. |
| `rabbitmq_list_bindings` | Exactly one of `queue` or `exchange`; optional `page`, `pageSize`. |
| `rabbitmq_list_consumers` | Optional `queue`, `page`, `pageSize`; ACK modes, prefetch and channel identifiers. |
| `rabbitmq_inspect_channel` | Required `channelName`; channel-wide Unack and application connection details. |
| `rabbitmq_sample_messages` | Required `queue`; optional `count` (default 3, max 10). Fetches Ready messages and requeues them; changes delivery state and may affect ordering. Cannot inspect Unack bodies. |
| `rabbitmq_publish_message` | Required `exchange` (empty string allowed), `routingKey`, `payload`; optional `encoding`, string `headers`, `contentType`, `messageId`, `correlationId`, `persistent` (default true). Uses mandatory routing and publisher confirms. |
| `rabbitmq_consume_messages` | Required `queue`; optional `count`, `maxWaitMs`. Pulls and ACKs messages, removing them. `ackStatus=SENT` is not a broker receipt or business processing guarantee. |
| `kafka_connect` | Required comma-separated `bootstrapServers`; optional `securityProtocol`, `username`, `password`. Validates metadata, not every resource permission. |
| `kafka_list_topics` | Optional `nameContains`, `includeInternal` (false), `limit`. |
| `kafka_inspect_topic` | Required `topic`; optional `limit`; partition leaders, replicas, ISR and retained offset boundaries. |
| `kafka_list_consumer_groups` | Optional `nameContains`, `limit`. |
| `kafka_inspect_consumer_group` | Required `groupId`; optional `topic`, `limit`; members, assignments, committed/end offsets and lag. Missing or out-of-range offsets are not zero lag. |
| `kafka_read_records` | Required `topic`, `partition`; optional `start` (`latest`, `earliest`, `offset`, `timestamp`), `offset`, `timestampMs`, `count`, `maxWaitMs`. No group membership or commits. Latest reads a recent offset window, not necessarily exactly N records. |
| `kafka_publish_record` | Required `topic`, `payload`; optional string `key`, `partition`, `encoding`, string `headers`. Returns broker partition/offset/timestamp. |
| `kafka_consume_records` | Required `topic`, `groupId`; optional `count`, `maxWaitMs`, `commit` (false), `offsetReset` (`earliest`, `latest`, `none`; default earliest). Joins that group and may rebalance other consumers. Commits only returned records when explicitly requested. |

Message encoding is `utf8` (default) or `base64`. Reads preserve message identifiers,
correlation headers and Kafka topic/partition/offset. JSON bodies remain original text.
Defaults: list size 50/max 100; reads/consumes 10/max 100; preview 4096 bytes;
publish payload max 65536 bytes; message result max 1 MiB; wait 5s/max 15s.
Configure limits under `omni.rabbitmq.limits` / `omni.kafka.limits`, and timeouts
under each connector's `timeouts` (`connect-ms`, `operation-ms`, `max-wait-ms`).

Publisher confirmation does not mean business processing succeeded. Unknown publish,
ACK or commit outcomes must not be blindly retried. Kafka producer idempotence does
not deduplicate repeated MCP tool calls. Kafka lag is an offset distance, not proof of
business processing or an exact message count. Consumers are short-lived; no persistent
subscription or cross-call ACK handle exists. No resource creation, purge or offset reset
is exposed. Existing topics/exchanges/queues and suitable permissions are required.

Examples (connect first, refresh tools):

```json
{"name":"rabbitmq_connect","arguments":{"managementUrl":"http://localhost:15672","username":"debug","password":"<password>","amqpAddresses":"localhost:5672"}}
{"name":"rabbitmq_inspect_queue","arguments":{"queue":"orders"}}
{"name":"rabbitmq_publish_message","arguments":{"exchange":"","routingKey":"orders","payload":"{\"orderId\":123}","contentType":"application/json","correlationId":"debug-123"}}
{"name":"kafka_connect","arguments":{"bootstrapServers":"localhost:9092"}}
{"name":"kafka_read_records","arguments":{"topic":"orders","partition":0,"start":"latest","count":3}}
{"name":"kafka_consume_records","arguments":{"topic":"orders","groupId":"debug-orders","commit":false,"count":3}}
```

For backlog diagnostics, inspect queues/groups, then consumers/channels or partitions,
then correlate sampled records with application logs using their existing identifiers.
To exercise a partial business flow, publish a test event or explicitly consume as a
stand-in. Using a business Kafka group affects its assignment and, when commit=true,
its future consumption position.

Validation boundary: default tests use local HTTP fixtures and client test doubles;
no broker/container is required. Core live scenarios also passed on RabbitMQ 3.6.5
and a local PLAINTEXT Kafka broker whose server version was not identified. Kafka
SASL/ACL, fault injection and additional broker-version compatibility remain unverified.

Live broker tests are opt-in and require explicit permission to create and delete
isolated test resources. Build the current JAR with `mvn package` first and use JDK 25
and Python 3. From the repository root:

```powershell
# RabbitMQ: requires a management account that can create vhosts and permissions.
$env:OMNI_RABBITMQ_ACCEPTANCE='true'
$env:RABBITMQ_MANAGEMENT_URL='http://<host>:15672'
$env:RABBITMQ_AMQP_ADDRESSES='<host>:5672'
$env:RABBITMQ_USERNAME='<test-user>'
$env:RABBITMQ_PASSWORD='<test-password>'
mvn test -pl omni-connect-app -am '-DskipTests=false' '-Dtest=RabbitMqBrokerTest' '-Dsurefire.failIfNoSpecifiedTests=false'
python tests/test_rabbitmq_stdio.py
$env:OMNI_RABBITMQ_ACCEPTANCE=$null

# Kafka: requires permissions to manage temporary topics and consumer groups.
$env:OMNI_KAFKA_ACCEPTANCE='true'
$env:KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
mvn test -pl omni-connect-app -am '-DskipTests=false' '-Dtest=KafkaBrokerTest' '-Dsurefire.failIfNoSpecifiedTests=false'
$env:OMNI_KAFKA_ACCEPTANCE=$null
```

Fixtures use unique `omni-mcp-acceptance-` names and clean up their own resources.
`KafkaBrokerTest` also runs `tests/test_kafka_stdio.py` against its fixtures and checks
the resulting commit independently. Keep credentials out of Git and clear both opt-in
flags before running the default offline suite.
