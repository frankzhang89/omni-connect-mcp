package xyz.fz.omni.config.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;

public record MigrationProperties(
        @Valid @NotNull From from, @Valid @NotNull To to, @Valid Http http) {
    /** Supplies HTTP timeout defaults when the entire HTTP configuration block is omitted. */
    public MigrationProperties {
        if (http == null) http = new Http(null, null);
    }

    public record From(@Valid @NotNull Apollo apollo) {}

    public record To(@Valid @NotNull Nacos nacos) {}

    public record Apollo(
            @NotBlank String meta,
            @NotBlank String appId,
            String cluster,
            String namespace,
            String fileType,
            String accessKey,
            String portalUrl,
            String openapiToken,
            String env,
            String operator,
            String releasedBy) {
        /** Retains the original read-only construction API. */
        public Apollo(
                String meta,
                String appId,
                String cluster,
                String namespace,
                String fileType,
                String accessKey) {
            this(
                    meta, appId, cluster, namespace, fileType, accessKey, null, null, null, null,
                    null);
        }

        /** Applies Apollo defaults only to omitted values, preserving explicit configuration. */
        @org.springframework.boot.context.properties.bind.ConstructorBinding
        public Apollo {
            if (cluster == null) cluster = "default";
            if (namespace == null) namespace = "application";
            if (fileType == null) fileType = "properties";
        }
    }

    public record Nacos(
            @NotBlank String serverAddr,
            String contextPath,
            String username,
            String password,
            String namespace,
            String group,
            String fileType) {
        /** Applies Nacos defaults and normalizes public to its empty wire identifier. */
        public Nacos {
            if (contextPath == null) contextPath = "/nacos";
            if (namespace == null || namespace.equals("public")) namespace = "";
            if (group == null) group = "DEFAULT_GROUP";
            if (fileType == null) fileType = "yaml";
        }
    }

    public record Http(Duration connectTimeout, Duration readTimeout) {
        /** Sets bounded timeout defaults and rejects zero or negative durations. */
        public Http {
            if (connectTimeout == null) connectTimeout = Duration.ofSeconds(3);
            if (readTimeout == null) readTimeout = Duration.ofSeconds(10);
            if (connectTimeout.isNegative()
                    || connectTimeout.isZero()
                    || readTimeout.isNegative()
                    || readTimeout.isZero()) {
                throw new IllegalArgumentException("HTTP timeouts must be positive");
            }
        }
    }
}
