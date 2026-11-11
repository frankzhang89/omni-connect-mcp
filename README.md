# Omni Connect MCP

A modular MCP server for Apollo, Nacos 2.x, JDBC databases, and standalone Redis. One deployment contains one executable JAR and one external `application.yml`. Java 25+, Maven 3.6.3+, Spring Boot 4.1.1, and Spring AI 2.0.1 are used.

## Build and deploy

Select JDK 25 before building:

```powershell
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-25.0.1'
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
mvn -B -ntp clean package
```

Packaging exports only `omni-connect-mcp.jar` to `target/release/omni-connect-mcp/`. Library JARs and `.jar.original` files stay outside this directory. For deployment, copy the JAR to its destination and separately copy `application.yml` and any required launchers from the project root. Edit the external YAML; it is not embedded in the JAR. Set `OMNI_AUTH_TOKEN` to a real shared Bearer token before HTTP startup, or explicitly disable `omni.auth.enabled`.

Override the parent POM's `base.export.path` property to export the runnable JAR elsewhere:

```powershell
mvn clean package '-Dbase.export.path=E:/opt/packaging'
```

This creates `E:/opt/packaging/omni-connect-mcp/omni-connect-mcp.jar`. No configuration, launchers, or documentation are exported. The default base is `${maven.multiModuleProjectDirectory}/target/release`. Compilation and intermediate module artifacts remain in each module's `target/`. Export also runs during `verify` and replaces the exported JAR. Maven `clean` cleans build artifacts, not a custom external export directory. Files exported by earlier versions remain in an existing external directory until removed manually.

Build recursively from the root POM. In IntelliJ IDEA, disable the Maven **Non-recursive** option. An invocation containing `--non-recursive` or `-N` builds only the root aggregator (`packaging=pom`) and can report success without producing an executable JAR. A complete build lists the root project and all five modules in its reactor summary.

In PowerShell, quote the entire `-Dbase.export.path=...` argument as shown above. An unquoted dotted property can be split into separate arguments, producing an error such as `No plugin found for prefix '.export.path=E'` before packaging starts. Run `mvn -version` to confirm that Maven itself uses Java 25+, rather than an older default JDK. Launcher JDK settings do not select Maven's JDK; an IDE's Maven runner must also use JDK 25+.

```powershell
$env:OMNI_AUTH_TOKEN = '<your-secret-token>'
.\omni-connect.cmd start
.\omni-connect.cmd stop
.\omni-connect.cmd restart --server.port=8286
```

```bash
export OMNI_AUTH_TOKEN='<your-secret-token>'
bash ./omni-connect.sh start
bash ./omni-connect.sh stop
bash ./omni-connect.sh restart --server.port=8286
```

The HTTP endpoint defaults to `http://localhost:8285/mcp`. HTTP launchers force HTTP mode, validate the replacement before restart, identify their own process using a deployment-specific JVM marker, and leave client-owned stdio children alone. Linux requires Bash 4.4+, `flock`, `setsid`, `readlink`, and `ss`. HTTP lifecycle operations are serialized per deployment directory. Use separate deployment directories and ports for multiple managed HTTP services.

On Windows, double-click `omni-connect.cmd` to open the menu. Up/Down and 1/2/3 select an action; Enter executes it immediately. Restart is selected initially. Five seconds without keyboard input executes the currently selected action; navigation resets the countdown. Explicit actions and redirected invocations skip the menu. Closing the launcher after startup leaves Java running.

### Select a JDK

HTTP scripts accept `--java-home=<absolute JDK directory or environment-variable name>`. Alternatively, set `$CustomJavaHome` in PowerShell or `CUSTOM_JAVA_HOME` in Bash to either form. Selection order is the command-line setting, script setting, `OMNI_JAVA_HOME`, `JAVA_HOME`, then PATH. An explicitly named environment variable must exist. Java 25+ is checked before an existing service is stopped. Paths containing spaces must be quoted.

```powershell
.\omni-connect.cmd restart --java-home=JDK25
.\omni-connect.cmd restart "--java-home=C:\Program Files\Java\jdk-25.0.1"
```

## Stdio and multiple processes

The same JAR and YAML support HTTP and any number of stdio processes simultaneously. Each stdio client must own its child's stdin/stdout. A stdio process never opens an HTTP port, uses no HTTP Bearer token, and exits after stdin EOF. Diagnostic output goes to stderr and PID-specific files under `log/`; stdout contains MCP messages only.

```json
{
  "mcpServers": {
    "omni-stdio": {
      "command": "C:/Program Files/Java/jdk-25.0.1/bin/java.exe",
      "args": [
        "-jar", "E:/deploy/omni/omni-connect-mcp.jar",
        "--spring.config.location=file:E:/deploy/omni/application.yml",
        "--spring.ai.mcp.server.stdio=true"
      ]
    }
  }
}
```

Register multiple entries with distinct names to start multiple children. Optional `omni-connect-stdio.cmd` / `omni-connect-stdio` wrappers also support a script-level JDK environment-variable name or absolute directory. They inherit the protocol streams and use the adjacent YAML. On Unix, run the wrapper with `sh /absolute/path/omni-connect-stdio` or mark it executable.

Each process owns independent connection state, Redis database selection, JDBC pools, transactions, and token caches. Runtime changes affect all MCP sessions within that process only. Shared upstream data remains shared. HTTP stop/restart never manages stdio processes.

## Connections and tool discovery

All four YAML `enabled` flags default to `false`. They control startup activation; they do not prohibit later agent activation. Disabled connections require no address or credentials and create no upstream resources. `convert_config` always works offline.

Six tools are initially visible: `connector_status`, `apollo_connect`, `nacos_connect`, `db_switch_datasource`, `redis_connect`, and `convert_config`. Activation adds business tools to the existing server and sends tool-list change notifications. Refresh the client's tools, or reconnect if it does not refresh automatically. All connections active exposes 26 tools, preserving the original 22 business/switch/conversion tools.

| Connection tool | Required parameters | Optional parameters |
| --- | --- | --- |
| `apollo_connect` | `meta`, `appId` | `cluster`, `namespace`, `fileType`, `accessKey`, `portalUrl`, `openapiToken`, `env`, `operator`, `releasedBy` |
| `nacos_connect` | `serverAddr` | `contextPath`, `username`, `password`, `namespace`, `group`, `fileType` |
| `db_switch_datasource` | `jdbcUrl`, `username`, `password` | `schema` |
| `redis_connect` | `host` | `port`, `username`, `password`, `database` |

Runtime configuration is memory-only and never rewrites YAML. Omitted connection fields use type defaults, not previous credentials. Failed validation leaves the current connection usable. Apollo probes a published default namespace; Nacos probes namespace discovery; JDBC validates the candidate pool; Redis uses PING. Portal item permissions are validated when used. Startup preserves lazy access for Apollo/Nacos/Redis; enabled JDBC must validate before startup succeeds. Status reports activation, not continuous upstream health. Runtime disable is outside this version.

## Configuration migration

Nacos namespace discovery first uses `/v2/console/namespace/list` (response code `0`). When that endpoint returns HTTP 404, it retries the read through `/v1/console/namespaces` (response code `200`) for older servers. Authentication failures, server errors, and malformed responses do not trigger version fallback. Configuration reads and publication continue to use `/v1/cs/configs`. This compatibility covers the V1/V2 APIs; Nacos 3.x console APIs are not covered.

| Previous prefix | New prefix |
| --- | --- |
| `migrate.http` | `omni.config.http` |
| `migrate.from.apollo` | `omni.config.apollo` |
| `migrate.to.nacos` | `omni.config.nacos` |
| `dbmcp.auth` | `omni.auth` |
| `dbmcp.database` | `omni.jdbc.connection` |
| `dbmcp.switch`, `dbmcp.query`, `dbmcp.transaction`, `dbmcp.write` | Corresponding children under `omni.jdbc` |
| `spring.data.redis` | `omni.redis.connection` |
| `dbmcp.redis` | `omni.redis.limits` |

Old aliases are not supported. Standard `spring.*` and `server.*` keys retain their normal meaning. Conversion defaults use the YAML Apollo/Nacos file types, falling back to properties/YAML even if those providers are inactive. Credentials in the checked-in YAML are empty environment placeholders; keep deployment secrets private.

## Modules and extension

`omni-connect-core` owns shared transport, authentication, connector lifecycle, and tool registration. `omni-connect-config`, `omni-connect-jdbc`, and `omni-connect-redis` own their protocols and business logic. `omni-connect-app` assembles the runnable application. Every module uses group ID `xyz.fz` and packages under `xyz.fz.omni`.

To add MQ later, create a new module depending on core, implement `ConnectorModule`, expose its typed management endpoint, and publish validated `ConnectorRegistry.Target` instances with their tool specifications and cleanup callback. Add the module dependency to the application and rebuild the same executable JAR. Business modules must not depend on one another. This version includes no MQ implementation or runtime plugin loading.

Read [AGENT_GUIDE.md](AGENT_GUIDE.md) for tool semantics and recovery. Tests use local HTTP peers, H2 only in test scope, and a controlled Redis fixture; they do not establish compatibility with private production services. The retained Postman simulation under `postman/` is optional and is not needed for the default test suite.

## Verification

### GitHub Actions

Pushing a tag beginning with `v` (for example, `v0.1.0`) triggers `.github/workflows/tag-build.yml`. Branch pushes and pull requests do not trigger this workflow. The Ubuntu runner installs Temurin JDK 25, runs `mvn clean verify -DskipTests=false`, exercises the runnable JAR and launchers, and creates two distribution archives. After successful verification and artifact upload, it publishes a GitHub Release for the existing tag with generated release notes and attaches both archives. Rerunning the workflow replaces attachments with the same names. The workflow uses the built-in `GITHUB_TOKEN` with `contents: write`; no additional token or private service credentials are needed. Actions artifacts are retained for 30 days and failure diagnostics for 7 days; Release attachments are independent of artifact retention.

| Artifact | Archive | Contents under `omni-connect-mcp/` |
| --- | --- | --- |
| `omni-connect-mcp-<tag>-windows` | `omni-connect-mcp-<tag>-windows.zip` | Runnable JAR, example `application.yml`, `omni-connect.cmd`, `omni-connect.ps1`, `omni-connect-stdio.cmd`, `omni-connect-stdio.ps1` |
| `omni-connect-mcp-<tag>-linux` | `omni-connect-mcp-<tag>-linux.tar.gz` | Runnable JAR, example `application.yml`, `omni-connect.sh`, `omni-connect-stdio` |

Linux launchers have executable permissions in the tar archive. Both packages require an installed JDK 25+; neither bundles a JDK. Download the named platform archives directly from the [Releases page](https://github.com/frankzhang89/omni-connect-mcp/releases). GitHub's automatically generated `Source code` archives contain repository source, not the runnable application. Actions artifacts remain available as a secondary download; GitHub wraps each in its own download ZIP. Local Maven packaging still exports only the runnable JAR. To create both archives locally after building, run `python scripts/package_release.py v0.1.0`; outputs appear under `target/dist/`.

Commit the intended source first, then create and push its tag:

```bash
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin main
git push origin v0.1.0
```

`mvn clean verify -DskipTests=false` runs the unit and application tests and exports the runnable JAR. Tests are skipped by default through the parent POM property. Test-only H2 is not bundled. To exercise the actual exported JAR, HTTP lifecycle scripts, authentication, a Windows stdio wrapper, and concurrent stdio children, run `python tests/test_release.py` with `JAVA_HOME` pointing to JDK 25. The release smoke test combines the exported JAR with configuration and launchers from the project root in its own deployment under `target/`, uses an ephemeral HTTP port, and stops only the process it launched. On Unix it exercises the corresponding shell wrappers; run that check on a Unix host with the documented utilities installed.
