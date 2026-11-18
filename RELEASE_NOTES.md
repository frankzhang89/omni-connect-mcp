# Release Notes

## v0.2.5

- Added `db_execute_permission` for native grants, revocations, account and password administration, and role management across MySQL, PostgreSQL, Oracle, SQL Server, and StarRocks. Login status and host-qualified accounts follow each database's capabilities.
- Added `db_execute_supreme` to pass raw SQL to JDBC and return ordered query results and update counts. Each call isolates session state by discarding its connection afterwards.
- Both tools are disabled by default. Enable them independently with `omni.jdbc.write.allow-permissions` or `omni.jdbc.write.allow-supreme`.
- Require complete SQL parsing in guarded JDBC tools and route account and role drops through the permission tool. Redact statement errors before the connection pool logs them.
- Verified raw batches, session isolation, permission changes, account locking, and password rotation against MySQL 5.7.12.

## v0.2.0

- Added RabbitMQ connectivity through the Management HTTP API and optional AMQP connections.
- Added RabbitMQ tools to inspect queues, bindings, consumers, and channels; sample ready messages; publish test messages; and consume bounded batches with acknowledgements.
- Added Kafka connectivity using PLAINTEXT or SASL_PLAINTEXT with the PLAIN mechanism.
- Added Kafka tools to inspect topics, partitions, consumer groups, and lag; read records without committing offsets; publish records; and perform bounded group consumption with optional offset commits.
- Added runtime activation and connection replacement for both connectors, with failed replacements retaining the existing connection.
- Added offline regression tests and opt-in live broker tests for HTTP MCP and stdio.
- Added automatic publication of version-specific release notes alongside Windows and Linux binaries on GitHub Releases, starting with v0.2.0. Tagged builds validate the project version and require a unique, non-empty release notes section before publishing.

## v0.1.1

- Added support for Nacos 1.x, 2.x, and 3.x, with automatic server version detection and version-specific API selection.

## v0.1.0

- Initial release of Omni Connect MCP.
