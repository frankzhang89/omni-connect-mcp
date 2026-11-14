# Release Notes

Release notes are written in English, with the newest version first.

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
