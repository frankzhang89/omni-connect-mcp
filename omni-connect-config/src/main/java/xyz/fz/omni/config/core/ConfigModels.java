package xyz.fz.omni.config.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

public final class ConfigModels {
    /** Prevents construction of this holder for shared request and result types. */
    private ConfigModels() {}

    public enum Provider {
        APOLLO,
        NACOS,
        ETCD
    }

    public enum Capability {
        READ,
        WRITE,
        READ_ITEM,
        UPDATE_ITEM,
        DISCOVER
    }

    public record NamespaceInfo(String namespace, String namespaceShowName, long configCount) {}

    public record NamespaceListResult(List<NamespaceInfo> namespaces) {}

    public record DataIdCountResult(
            String namespace, String group, long totalCount, List<String> dataIds) {}

    public record Location(
            Provider provider,
            String appId,
            String cluster,
            String namespace,
            String group,
            String dataId) {}

    public record ReadRequest(Location location, FileType fileType) {}

    public record WriteRequest(Location location, FileType fileType, String content) {}

    public record ReadResult(
            Location location, String fileType, String content, String sha256, String releaseKey) {}

    public record PublishResult(
            Location location, String fileType, boolean published, String sha256) {}

    public record ConversionResult(
            String sourceType,
            String targetType,
            boolean converted,
            String content,
            String sha256) {}

    public record ItemReadRequest(Location location, String env, String key) {}

    public record ItemReadResult(
            Location location,
            String env,
            String fileType,
            String key,
            String value,
            String comment,
            String modifiedBy,
            String modifiedTime,
            String sha256,
            String valueSource) {}

    public record ItemUpdateRequest(
            ItemReadRequest item,
            String value,
            boolean publish,
            String operator,
            String releasedBy,
            String releaseTitle,
            String releaseComment) {}

    public record ItemUpdateResult(
            Location location,
            String env,
            String fileType,
            String key,
            String sha256,
            boolean updated,
            boolean published,
            boolean verified) {}

    /** Hashes exact UTF-8 content so agents can compare publication and read-back results. */
    public static String sha256(String content) {
        try {
            return HexFormat.of()
                    .formatHex(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(content.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable");
        }
    }

    /** Requires a nonblank identifier while preserving its original characters. */
    public static String required(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new MigrationException(
                    MigrationException.Code.INVALID_ARGUMENT, name + " is required");
        }
        return value;
    }
}
