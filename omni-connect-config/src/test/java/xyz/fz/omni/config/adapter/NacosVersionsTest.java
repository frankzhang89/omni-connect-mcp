package xyz.fz.omni.config.adapter;

import static org.assertj.core.api.Assertions.*;

import static xyz.fz.omni.config.core.ConfigModels.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import xyz.fz.omni.config.config.MigrationProperties;
import xyz.fz.omni.config.core.*;
import xyz.fz.omni.config.format.ConfigFormatService;
import xyz.fz.omni.config.support.UpstreamFixture;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;

class NacosVersionsTest {
    private MigrationProperties properties(UpstreamFixture peer, Duration timeout) {
        return new MigrationProperties(
                new MigrationProperties.From(
                        new MigrationProperties.Apollo(null, null, null, null, null, null)),
                new MigrationProperties.To(
                        new MigrationProperties.Nacos(
                                peer.base(),
                                "/nacos",
                                "user",
                                "password",
                                "configured-ns",
                                "GROUP",
                                "yaml")),
                new MigrationProperties.Http(Duration.ofSeconds(1), timeout));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "1.4.8",
                "2.0.0",
                "2.1.2",
                "2.2.0",
                "2.4.3",
                "3.0.0",
                "3.2.0",
                "3.2.0-SNAPSHOT",
                "2.4.0.1",
                "2.2.0-BETA"
            })
    void routesAllOperationsAndConcurrentReadsUsingOnlyTheCachedVersion(String version)
            throws Exception {
        try (var peer = new UpstreamFixture()) {
            peer.nacosVersion = version;
            var props = properties(peer, Duration.ofSeconds(2));
            try (var http = new HttpTransport(props)) {
                var store = new NacosConfigStore(http, props);
                var service = new ConfigService(props, new ConfigFormatService(), List.of(store));
                assertThat(store.version()).isEqualTo(version);
                assertThat(service.nacosListNamespaces().namespaces())
                        .extracting(NamespaceInfo::namespace)
                        .containsExactly("", "empty-ns");
                String content =
                        "# original\r\napp:\r\n  value: '中文 & + = % ${HOST:localhost}'\r\n";
                String id = "app + &.yaml";
                String group = "group + &";
                service.nacosPublish(id, content, group, "public", null);
                assertThat(service.nacosRead(id, group, "", null).content()).isEqualTo(content);
                assertThat(service.nacosRead(id, group, "public", null).sha256())
                        .isEqualTo(sha256(content));
                assertThat(service.nacosCountDataIds("", group).dataIds()).containsExactly(id);
                service.nacosPublish("empty", "", null, null, null);
                assertThat(service.nacosRead("empty", null, null, null).content()).isEmpty();
                service.nacosPublish("properties", "key=中文\r\n", null, null, "properties");
                assertThat(service.nacosRead("properties", null, null, "properties").content())
                        .isEqualTo("key=中文\r\n");
                assertThatThrownBy(() -> service.nacosRead("missing", null, null, null))
                        .hasMessageContaining("NOT_FOUND");
                // Populate an isolated namespace/group with enough data to require two pages.
                for (int i = 0; i < 101; i++)
                    peer.configurations.put("other|OTHER|app-" + i, content);
                assertThat(service.nacosCountDataIds("other", "OTHER").totalCount()).isEqualTo(101);
                assertThat(service.nacosCountDataIds("other", "OTHER").dataIds()).hasSize(101);
                assertThat(service.nacosCountDataIds("other", group).totalCount()).isZero();
                try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                    var futures = new ArrayList<java.util.concurrent.Future<String>>();
                    for (int i = 0; i < 8; i++)
                        futures.add(
                                executor.submit(
                                        () -> service.nacosRead(id, group, "", null).content()));
                    for (var future : futures) assertThat(future.get()).isEqualTo(content);
                }
                assertThat(peer.stateV3Reads).hasValue(1);
                assertThat(peer.stateV1Reads).hasValue(version.startsWith("3.") ? 0 : 1);
                assertThat(peer.logins).hasValue(1);
                var api = NacosServerVersion.parse(version).api();
                assertThat(
                                peer.nacosRequests.stream()
                                        .filter(r -> r.path().contains("/namespace"))
                                        .map(UpstreamFixture.NacosRequest::path))
                        .containsExactly("/nacos" + api.namespaces);
                assertThat(
                                peer.nacosRequests.stream()
                                        .filter(r -> r.path().endsWith("/login"))
                                        .map(UpstreamFixture.NacosRequest::path))
                        .containsExactly("/nacos" + api.loginPath());
                assertThat(
                                peer.nacosRequests.stream()
                                        .filter(r -> r.path().contains("/cs/"))
                                        .map(UpstreamFixture.NacosRequest::path))
                        .allMatch(
                                path ->
                                        path.equals("/nacos" + api.configPath())
                                                || path.equals("/nacos" + api.listPath()));
                if (version.startsWith("3.")) {
                    assertThat(
                                    peer.nacosRequests.stream()
                                            .filter(
                                                    r ->
                                                            r.path().contains("/cs/")
                                                                    || r.path()
                                                                            .contains(
                                                                                    "/namespace")))
                            .allSatisfy(
                                    r -> {
                                        assertThat(r.token()).startsWith("test-token-");
                                        assertThat(r.query()).doesNotContainKey("accessToken");
                                        assertThat(r.form()).doesNotContainKey("accessToken");
                                    });
                }
                // Business endpoint failures must never trigger discovery or cross-version
                // fallback.
                peer.readStatus = 410;
                assertThatThrownBy(() -> service.nacosRead(id, group, "", null))
                        .hasMessageContaining("UPSTREAM_ERROR");
                assertThat(peer.stateV3Reads).hasValue(1);
                peer.namespaceV1Status = peer.namespaceV2Status = peer.namespaceV3Status = 404;
                assertThatThrownBy(service::nacosListNamespaces).hasMessageContaining("NOT_FOUND");
                assertThat(
                                peer.nacosRequests.stream()
                                        .filter(r -> r.path().contains("/namespace"))
                                        .map(UpstreamFixture.NacosRequest::path))
                        .containsOnly("/nacos" + api.namespaces);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {404, 410})
    void onlyMissingStateEndpointsAllowLegacyDetection(int status) throws Exception {
        try (var peer = new UpstreamFixture()) {
            peer.stateV3Status = status;
            var props = properties(peer, Duration.ofSeconds(2));
            try (var http = new HttpTransport(props)) {
                assertThat(new NacosConfigStore(http, props).version()).isEqualTo("2.4.3");
                assertThat(peer.stateV3Reads).hasValue(1);
                assertThat(peer.stateV1Reads).hasValue(1);
                assertThat(peer.logins).hasValue(0);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {401, 403, 500})
    void stateErrorsFailActivationWithoutFallback(int status) throws Exception {
        try (var peer = new UpstreamFixture()) {
            peer.stateV3Status = status;
            var props = properties(peer, Duration.ofSeconds(2));
            try (var http = new HttpTransport(props)) {
                assertThatThrownBy(() -> new NacosConfigStore(http, props))
                        .hasMessageContaining(
                                status == 500 ? "UPSTREAM_ERROR" : "AUTHENTICATION_FAILED");
                assertThat(peer.stateV1Reads).hasValue(0);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "{}",
                "{\"version\":\"\"}",
                "{\"version\":3}",
                "{\"version\":\"3.2\"}",
                "{\"version\":\"3.x.0\"}",
                "{\"version\":\"99999999999.0.0\"}",
                "{\"code\":500,\"data\":{\"version\":\"3.0.0\"}}",
                "{\"code\":0,\"data\":null}",
                "<html>login</html>"
            })
    void invalidStateNeverGuessesAVersion(String body) throws Exception {
        try (var peer = new UpstreamFixture()) {
            peer.stateV3Status = 200;
            peer.stateBody = body;
            var props = properties(peer, Duration.ofSeconds(2));
            try (var http = new HttpTransport(props)) {
                assertThatThrownBy(() -> new NacosConfigStore(http, props))
                        .hasMessageContaining("UPSTREAM_ERROR");
                assertThat(peer.stateV1Reads).hasValue(0);
            }
        }
    }

    @Test
    void acceptsTopLevelV3StateButRejectsUnknownMajorVersionsAndTimeouts() throws Exception {
        try (var peer = new UpstreamFixture()) {
            peer.nacosVersion = "3.2.0";
            peer.stateBody = "{\"version\":\"3.2.0\"}";
            var props = properties(peer, Duration.ofMillis(150));
            try (var http = new HttpTransport(props)) {
                assertThat(new NacosConfigStore(http, props).version()).isEqualTo("3.2.0");
                peer.stateBody = "{\"version\":\"4.0.0\"}";
                assertThatThrownBy(() -> new NacosConfigStore(http, props))
                        .hasMessageContaining("UNSUPPORTED_CAPABILITY");
                peer.stateDelayMillis = 500;
                assertThatThrownBy(() -> new NacosConfigStore(http, props))
                        .hasMessageContaining("TIMEOUT");
                assertThat(peer.stateV1Reads).hasValue(0);
            }
        }
    }

    @Test
    void v3RefreshesReadsButNeverReplaysPublicationsOrChangesApi() throws Exception {
        try (var peer = new UpstreamFixture()) {
            peer.nacosVersion = "3.0.0";
            var props = properties(peer, Duration.ofSeconds(2));
            try (var http = new HttpTransport(props)) {
                var store = new NacosConfigStore(http, props);
                var service = new ConfigService(props, new ConfigFormatService(), List.of(store));
                service.nacosPublish("a", "key: value", null, null, null);
                peer.rejectFirstRead = true;
                service.nacosRead("a", null, null, null);
                assertThat(peer.logins).hasValue(2);
                peer.publishStatus = 403;
                assertThatThrownBy(() -> service.nacosPublish("a", "key: value", null, null, null))
                        .hasMessageContaining("AUTHENTICATION_FAILED");
                assertThat(peer.writes).hasValue(2);
                peer.publishStatus = 200;
                peer.publishReply = "false";
                assertThatThrownBy(() -> service.nacosPublish("a", "key: value", null, null, null))
                        .hasMessageContaining("PUBLISH_FAILED");
                peer.configBody = "{\"code\":1,\"data\":true}";
                assertThatThrownBy(() -> service.nacosPublish("a", "key: value", null, null, null))
                        .hasMessageContaining("UPSTREAM_ERROR");
                peer.configBody = "{\"code\":0,\"data\":{\"content\":null}}";
                assertThatThrownBy(() -> service.nacosRead("a", null, null, null))
                        .hasMessageContaining("UPSTREAM_ERROR");
                peer.configBody = "{\"code\":0,\"data\":{\"totalCount\":1,\"pageItems\":[]}}";
                assertThatThrownBy(() -> service.nacosCountDataIds(null, null))
                        .hasMessageContaining("UPSTREAM_ERROR");
                assertThat(peer.stateV3Reads).hasValue(1);
                assertThat(peer.stateV1Reads).hasValue(0);
            }
        }
    }
}
