package xyz.fz.omni.config.adapter;

import static org.assertj.core.api.Assertions.*;

import static xyz.fz.omni.config.core.MigrationException.Code.*;

import org.junit.jupiter.api.*;

import xyz.fz.omni.config.config.MigrationProperties;
import xyz.fz.omni.config.core.*;
import xyz.fz.omni.config.core.ConfigModels.*;
import xyz.fz.omni.config.format.ConfigFormatService;
import xyz.fz.omni.config.mcp.ConfigTools;
import xyz.fz.omni.config.support.UpstreamFixture;

import java.time.Duration;
import java.util.List;
import java.util.Map;

class ApolloItemsTest {
    private UpstreamFixture upstream;
    private ConfigService service;
    private static final String KEY = "authen.merchantName";

    @BeforeEach
    void start() throws Exception {
        upstream = new UpstreamFixture();
        service = service(Duration.ofSeconds(2), "editor", "publisher");
    }

    @AfterEach
    void stop() {
        upstream.close();
    }

    private ConfigService service(Duration timeout, String editor, String publisher) {
        var properties =
                new MigrationProperties(
                        new MigrationProperties.From(
                                new MigrationProperties.Apollo(
                                        upstream.apollo(),
                                        "source-app",
                                        null,
                                        null,
                                        null,
                                        null,
                                        upstream.apollo(),
                                        "test-openapi-token",
                                        "DEV",
                                        editor,
                                        publisher)),
                        new MigrationProperties.To(
                                new MigrationProperties.Nacos(
                                        upstream.base(), null, null, null, null, null, null)),
                        new MigrationProperties.Http(Duration.ofSeconds(1), timeout));
        var formats = new ConfigFormatService();
        return new ConfigService(
                properties,
                formats,
                List.of(new ApolloConfigStore(new HttpTransport(properties), properties, formats)));
    }

    private ItemReadResult read(String key) {
        return service.apolloReadItem(key, null, null, null, null, null);
    }

    private ItemUpdateResult update(String value, Boolean publish) {
        return service.apolloUpdate(
                KEY, value, null, null, null, null, null, publish, null, null, null, null);
    }

    private Map<String, String> draft() {
        return upstream.draft("DEV", "source-app", "default", "application");
    }

    private Map<String, String> active() {
        draft();
        return upstream.apolloReleases.get(
                upstream.portalNamespace("DEV", "source-app", "default", "application"));
    }

    @Test
    void readsEditingValueMetadataAndHashWithoutOperatorOrWrites() {
        draft().put(KEY, "unpublished");
        service = service(Duration.ofSeconds(2), null, null);
        var result = read(KEY);
        assertThat(result.value()).isEqualTo("unpublished");
        assertThat(result.valueSource()).isEqualTo("EDITING");
        assertThat(result.comment()).isEqualTo("original comment");
        assertThat(result.modifiedBy()).isEqualTo("editor");
        assertThat(result.modifiedTime()).isNotBlank();
        assertThat(result.sha256()).isEqualTo(ConfigModels.sha256("unpublished"));
        assertThat(result.env()).isEqualTo("DEV");
        assertThat(result.location().appId()).isEqualTo("source-app");
        assertThat(active().get(KEY)).isEqualTo("sqmallauth");
        assertThat(upstream.portalAuthorization).isEqualTo("test-openapi-token");
        assertThat(upstream.itemWrites).hasValue(0);
        assertThat(upstream.releaseWrites).hasValue(0);
    }

    @Test
    void modifiesPublishesAndReusesItemReadBeforeAndAfter() {
        var result = update("新商户 & + = %", null);
        assertThat(result.updated()).isTrue();
        assertThat(result.published()).isTrue();
        assertThat(result.verified()).isTrue();
        assertThat(result.sha256()).isEqualTo(ConfigModels.sha256("新商户 & + = %"));
        assertThat(upstream.itemReads).hasValue(2);
        assertThat(upstream.itemWrites).hasValue(1);
        assertThat(upstream.releaseWrites).hasValue(1);
        assertThat(upstream.lastItemWrite.comment()).isEqualTo("original comment");
        assertThat(upstream.lastItemWrite.operator()).isEqualTo("editor");
        assertThat(upstream.lastReleaseWrite).containsEntry("releasedBy", "publisher");
        assertThat(upstream.lastReleaseWrite.get("releaseTitle").toString())
                .startsWith("update-config-");
        assertThat(active())
                .containsEntry(KEY, "新商户 & + = %")
                .containsEntry("other.key", "unchanged");
    }

    @Test
    void onlySavesEmptyValueAndAllowsOtherUnpublishedChangesWithoutPublishing() {
        draft().put("other.key", "unpublished");
        var result = update("", false);
        assertThat(result.updated()).isTrue();
        assertThat(result.published()).isFalse();
        assertThat(read(KEY).value()).isEmpty();
        assertThat(active().get(KEY)).isEqualTo("sqmallauth");
        assertThat(upstream.releaseWrites).hasValue(0);
    }

    @Test
    void overridesIdentifiersAccountsAndReleaseMetadataWithSafePathEncoding() {
        String app = "app + &?";
        String key = "url + &?/%";
        upstream.draft("UAT", app, "cluster-x", "custom").put(key, "old");
        var result = service.apolloReadItem(key, app, "cluster-x", "custom", "properties", "UAT");
        assertThat(result.value()).isEqualTo("old");
        var updated =
                service.apolloUpdate(
                        key,
                        "new",
                        app,
                        "cluster-x",
                        "custom",
                        "properties",
                        "UAT",
                        false,
                        "custom-editor",
                        "custom-publisher",
                        "title",
                        "release comment");
        assertThat(updated.location()).isEqualTo(result.location());
        assertThat(upstream.lastItemWrite.key()).isEqualTo(key);
        assertThat(upstream.lastItemWrite.operator()).isEqualTo("custom-editor");
        service.apolloUpdate(
                KEY,
                "new",
                null,
                null,
                null,
                null,
                null,
                true,
                "custom-editor",
                "custom-publisher",
                "title",
                "release comment");
        assertThat(upstream.lastReleaseWrite)
                .containsEntry("releasedBy", "custom-publisher")
                .containsEntry("releaseTitle", "title")
                .containsEntry("releaseComment", "release comment");
    }

    @Test
    void supportsAgentReadReplacePortAndFullValueUpdate() {
        String previous = read("service.url").value();
        java.net.URI uri = java.net.URI.create(previous);
        String replacement =
                uri.getScheme()
                        + "://"
                        + uri.getHost()
                        + ":9090"
                        + uri.getRawPath()
                        + "?"
                        + uri.getRawQuery();
        service.apolloUpdate(
                "service.url",
                replacement,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        assertThat(read("service.url").value())
                .isEqualTo("https://api.example.com:9090/auth?enabled=true");
    }

    @Test
    void skipsIdenticalUpdatesAndReleasesButPublishesExistingTargetDraft() {
        var noOp = update("sqmallauth", true);
        assertThat(noOp.updated()).isFalse();
        assertThat(noOp.published()).isTrue();
        assertThat(upstream.itemWrites).hasValue(0);
        assertThat(upstream.releaseWrites).hasValue(0);
        draft().put(KEY, "already edited");
        assertThat(update("already edited", true).updated()).isFalse();
        assertThat(upstream.releaseWrites).hasValue(1);
        assertThat(active()).containsEntry(KEY, "already edited");
    }

    @Test
    void detectsOtherAddedRemovedAndChangedKeysBeforeWriting() {
        for (String mode : List.of("added", "removed", "changed")) {
            draft().clear();
            draft().putAll(UpstreamFixture.APOLLO_VALUES);
            if (mode.equals("added")) draft().put("new.key", "new");
            if (mode.equals("removed")) draft().remove("other.key");
            if (mode.equals("changed")) draft().put("other.key", "different");
            assertThatThrownBy(() -> update("new", true))
                    .isInstanceOfSatisfying(
                            MigrationException.class,
                            e -> {
                                assertThat(e.code()).isEqualTo(CONCURRENT_MODIFICATION);
                                assertThat(e.details())
                                        .containsEntry("stage", "PRECHECK")
                                        .containsEntry("updated", false);
                            });
        }
        assertThat(upstream.itemWrites).hasValue(0);
        assertThat(upstream.releaseWrites).hasValue(0);
    }

    @Test
    void detectsPendingChangesAgainAfterTheItemWasUpdated() {
        upstream.afterItemWrite = () -> draft().put("other.key", "concurrent");
        assertThatThrownBy(() -> update("new", true))
                .isInstanceOfSatisfying(
                        MigrationException.class,
                        e -> {
                            assertThat(e.code()).isEqualTo(CONCURRENT_MODIFICATION);
                            assertThat(e.details())
                                    .containsEntry("stage", "PREPUBLISH")
                                    .containsEntry("updated", true)
                                    .containsEntry("published", false);
                        });
        assertThat(upstream.releaseWrites).hasValue(0);
    }

    @Test
    void detectsTargetAndPublishedSnapshotMismatches() {
        upstream.afterItemWrite = () -> draft().put(KEY, "concurrent target");
        assertThatThrownBy(() -> update("new", true))
                .isInstanceOfSatisfying(
                        MigrationException.class,
                        e -> assertThat(e.code()).isEqualTo(VERIFICATION_FAILED));
        upstream.afterItemWrite = () -> {};
        upstream.beforeReleaseWrite = () -> draft().put("other.key", "raced after precheck");
        assertThatThrownBy(() -> update("new", true))
                .isInstanceOfSatisfying(
                        MigrationException.class,
                        e -> {
                            assertThat(e.code()).isEqualTo(VERIFICATION_FAILED);
                            assertThat(e.details())
                                    .containsEntry("stage", "VERIFY_RELEASE")
                                    .containsEntry("published", true);
                        });
    }

    @Test
    void handlesFirstReleaseWithoutPublishingOtherKeys() {
        active().clear();
        assertThatThrownBy(() -> update("new", true))
                .hasMessageContaining("CONCURRENT_MODIFICATION");
        draft().clear();
        draft().put(KEY, "old");
        assertThat(update("new", true).published()).isTrue();
        assertThat(active()).containsExactlyEntriesOf(Map.of(KEY, "new"));
    }

    @Test
    void missingKeyAndDeclaredOrActualYamlCannotBeWritten() {
        assertThatThrownBy(() -> read("missing")).hasMessageContaining("NOT_FOUND");
        assertThatThrownBy(
                        () ->
                                service.apolloUpdate(
                                        "missing", "new", null, null, null, null, null, null, null,
                                        null, null, null))
                .hasMessageContaining("NOT_FOUND");
        assertThatThrownBy(() -> service.apolloReadItem(KEY, null, null, null, "yaml", null))
                .hasMessageContaining("INVALID_FORMAT");
        upstream.namespaceFormat = "yaml";
        assertThatThrownBy(() -> read(KEY)).hasMessageContaining("INVALID_FORMAT");
        assertThatThrownBy(() -> update("new", true)).hasMessageContaining("INVALID_FORMAT");
        assertThat(upstream.itemWrites).hasValue(0);
    }

    @Test
    void rejectsInvalidInputsWithoutWritingAndPreservesErrorEnvelope() {
        var tools = new ConfigTools(service);
        var result =
                tools.apolloUpdate(
                        KEY, null, null, null, null, null, null, null, null, null, null, null);
        assertThat(result.isError()).isTrue();
        assertThat((Map<String, Object>) result.structuredContent())
                .containsEntry("code", "INVALID_ARGUMENT")
                .containsEntry("stage", "VALIDATE");
        assertThatThrownBy(() -> update("x".repeat(20001), true))
                .hasMessageContaining("INVALID_ARGUMENT");
        assertThatThrownBy(() -> read(" ")).hasMessageContaining("INVALID_ARGUMENT");
        assertThatThrownBy(() -> service.apolloReadItem(KEY, "", null, null, null, null))
                .hasMessageContaining("INVALID_ARGUMENT");
        assertThatThrownBy(
                        () ->
                                service.apolloUpdate(
                                        KEY,
                                        "x",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        true,
                                        null,
                                        null,
                                        "x".repeat(65),
                                        null))
                .hasMessageContaining("INVALID_ARGUMENT");
        assertThat(upstream.itemWrites).hasValue(0);
    }

    @Test
    void authAndLockRejectionsExposeNoUpstreamBodyAndNoRetry() {
        upstream.portalStatus = 403;
        assertThatThrownBy(() -> read(KEY))
                .hasMessageContaining("AUTHENTICATION_FAILED")
                .hasMessageNotContaining("PRIVATE_UPSTREAM_ERROR");
        upstream.portalStatus = 200;
        upstream.itemWriteStatus = 423;
        assertThatThrownBy(() -> update("x", true)).hasMessageContaining("CONCURRENT_MODIFICATION");
        assertThat(upstream.itemWrites).hasValue(1);
        upstream.itemWriteStatus = 200;
        upstream.releaseWriteStatus = 403;
        assertThatThrownBy(() -> update("x", true))
                .isInstanceOfSatisfying(
                        MigrationException.class,
                        e -> {
                            assertThat(e.code()).isEqualTo(AUTHENTICATION_FAILED);
                            assertThat(e.details())
                                    .containsEntry("updated", true)
                                    .containsEntry("published", false)
                                    .containsEntry("stage", "PUBLISH");
                        });
        assertThat(upstream.releaseWrites).hasValue(1);
    }

    @Test
    void updateTimeoutReportsUnknownUpdateWithoutRetryingOrPublishing() {
        service = service(Duration.ofMillis(150), "editor", "publisher");
        upstream.itemWriteDelay = 600;
        var result =
                new ConfigTools(service)
                        .apolloUpdate(
                                KEY, "new", null, null, null, null, null, true, null, null, null,
                                null);
        assertThat(result.isError()).isTrue();
        assertThat((Map<String, Object>) result.structuredContent())
                .containsEntry("code", "WRITE_RESULT_UNKNOWN")
                .containsEntry("stage", "UPDATE")
                .containsEntry("updated", null)
                .containsEntry("published", false);
        assertThat(upstream.itemWrites).hasValue(1);
        assertThat(upstream.releaseWrites).hasValue(0);
        assertThat(read(KEY).value()).isEqualTo("new");
    }

    @Test
    void releaseTimeoutReportsKnownUpdateAndUnknownPublicationWithoutRetry() {
        service = service(Duration.ofMillis(150), "editor", "publisher");
        upstream.releaseWriteDelay = 600;
        assertThatThrownBy(() -> update("new", true))
                .isInstanceOfSatisfying(
                        MigrationException.class,
                        e -> {
                            assertThat(e.code()).isEqualTo(WRITE_RESULT_UNKNOWN);
                            assertThat(e.details())
                                    .containsEntry("stage", "PUBLISH")
                                    .containsEntry("updated", true)
                                    .containsEntry("published", null);
                        });
        assertThat(upstream.releaseWrites).hasValue(1);
        assertThat(active()).containsEntry(KEY, "new");
    }

    @Test
    void missingNewConfigurationLeavesOriginalReadWorking() {
        var properties =
                new MigrationProperties(
                        new MigrationProperties.From(
                                new MigrationProperties.Apollo(
                                        upstream.apollo(), "source-app", null, null, null, null)),
                        new MigrationProperties.To(
                                new MigrationProperties.Nacos(
                                        upstream.base(), null, null, null, null, null, null)),
                        null);
        var formats = new ConfigFormatService();
        var original =
                new ConfigService(
                        properties,
                        formats,
                        List.of(
                                new ApolloConfigStore(
                                        new HttpTransport(properties), properties, formats)));
        assertThat(original.apolloRead(null, null, null, null).releaseKey()).isEqualTo("release-1");
        assertThatThrownBy(() -> original.apolloReadItem(KEY, null, null, null, null, null))
                .hasMessageContaining("portal-url");
    }
}
