package xyz.fz.omni.config.core;

import static xyz.fz.omni.config.core.ConfigModels.required;
import static xyz.fz.omni.config.core.MigrationException.Code.*;

import xyz.fz.omni.config.config.MigrationProperties;
import xyz.fz.omni.config.core.ConfigModels.*;
import xyz.fz.omni.config.format.ConfigFormatService;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class ConfigService {
    private final MigrationProperties settings;
    private final ConfigFormatService formats;
    private final Map<Provider, ConfigStore> stores = new EnumMap<>(Provider.class);

    /** Registers one adapter per provider so future implementations share the same routing. */
    public ConfigService(
            MigrationProperties settings, ConfigFormatService formats, List<ConfigStore> stores) {
        this.settings = settings;
        this.formats = formats;
        for (ConfigStore store : stores) {
            if (this.stores.putIfAbsent(store.provider(), store) != null) {
                throw new IllegalStateException("Duplicate configuration provider");
            }
        }
    }

    /**
     * Reads a published Apollo namespace using explicit identifiers or configured source defaults.
     */
    public ReadResult apolloRead(String appId, String cluster, String namespace, String fileType) {
        return readable(Provider.APOLLO).read(apolloRequest(appId, cluster, namespace, fileType));
    }

    /** Uses the same identity/default rules for document and item operations. */
    private ReadRequest apolloRequest(
            String appId, String cluster, String namespace, String fileType) {
        var defaults = settings.from().apollo();
        FileType type = FileType.parse(or(fileType, defaults.fileType()));
        String name = required(or(namespace, defaults.namespace()), "namespace");
        // Non-properties Apollo namespaces are addressed by their actual file-suffixed names.
        if (type == FileType.YAML && !(name.endsWith(".yaml") || name.endsWith(".yml"))) {
            name +=
                    or(fileType, defaults.fileType()).trim().equalsIgnoreCase("yml")
                            ? ".yml"
                            : ".yaml";
        }
        Location location =
                new Location(
                        Provider.APOLLO,
                        required(or(appId, defaults.appId()), "appId"),
                        required(or(cluster, defaults.cluster()), "cluster"),
                        name,
                        null,
                        null);
        return new ReadRequest(location, type);
    }

    public ItemReadResult apolloReadItem(
            String key,
            String appId,
            String cluster,
            String namespace,
            String fileType,
            String env) {
        ItemReadRequest request = apolloItemRequest(key, appId, cluster, namespace, fileType, env);
        ConfigStore store = stores.get(Provider.APOLLO);
        if (store instanceof ConfigStore.ItemReadable reader
                && store.capabilities().contains(Capability.READ_ITEM)) {
            return reader.readItem(request);
        }
        throw new MigrationException(UNSUPPORTED_CAPABILITY, "Apollo item read is not implemented");
    }

    public ItemUpdateResult apolloUpdate(
            String key,
            String value,
            String appId,
            String cluster,
            String namespace,
            String fileType,
            String env,
            Boolean publish,
            String operator,
            String releasedBy,
            String releaseTitle,
            String releaseComment) {
        ItemReadRequest item = apolloItemRequest(key, appId, cluster, namespace, fileType, env);
        if (value == null || value.length() > 20000)
            throw new MigrationException(
                    INVALID_ARGUMENT, "value is required and must not exceed 20000 characters");
        var defaults = settings.from().apollo();
        String editor = required(or(operator, defaults.operator()), "operator");
        boolean release = publish == null || publish;
        String configuredPublisher =
                defaults.releasedBy() == null || defaults.releasedBy().isBlank()
                        ? editor
                        : defaults.releasedBy();
        String publisher =
                release ? required(or(releasedBy, configuredPublisher), "releasedBy") : null;
        String title = or(releaseTitle, "update-config-" + java.time.Instant.now().toEpochMilli());
        String comment = or(releaseComment, "");
        if (release && (title.isBlank() || title.length() > 64 || comment.length() > 256)) {
            throw new MigrationException(
                    INVALID_ARGUMENT,
                    "releaseTitle must be nonblank and at most 64 characters; releaseComment at"
                        + " most 256");
        }
        ConfigStore store = stores.get(Provider.APOLLO);
        if (store instanceof ConfigStore.ItemUpdatable updater
                && store.capabilities().contains(Capability.UPDATE_ITEM)) {
            return updater.updateItem(
                    new ItemUpdateRequest(item, value, release, editor, publisher, title, comment));
        }
        throw new MigrationException(
                UNSUPPORTED_CAPABILITY, "Apollo item update is not implemented");
    }

    private ItemReadRequest apolloItemRequest(
            String key,
            String appId,
            String cluster,
            String namespace,
            String fileType,
            String env) {
        required(key, "key");
        if (key.length() > 128)
            throw new MigrationException(INVALID_ARGUMENT, "key must not exceed 128 characters");
        ReadRequest resolved = apolloRequest(appId, cluster, namespace, fileType);
        if (resolved.fileType() != FileType.PROPERTIES)
            throw new MigrationException(
                    INVALID_FORMAT, "Apollo item operations support properties only");
        var defaults = settings.from().apollo();
        required(defaults.portalUrl(), "omni.config.apollo.portal-url");
        required(defaults.openapiToken(), "omni.config.apollo.openapi-token");
        return new ItemReadRequest(
                resolved.location(), required(or(env, defaults.env()), "env"), key);
    }

    /** Reads the exact Nacos identity without detecting or converting its declared format. */
    public ReadResult nacosRead(String dataId, String group, String namespace, String fileType) {
        return readable(Provider.NACOS)
                .read(
                        new ReadRequest(
                                nacosLocation(dataId, group, namespace),
                                FileType.parse(or(fileType, settings.to().nacos().fileType()))));
    }

    public NamespaceListResult nacosListNamespaces() {
        return nacosDiscovery().listNamespaces();
    }

    public DataIdCountResult nacosCountDataIds(String namespace, String group) {
        return nacosDiscovery().countDataIds(nacosNamespace(namespace), nacosGroup(group));
    }

    private ConfigStore.Discoverable nacosDiscovery() {
        ConfigStore store = stores.get(Provider.NACOS);
        if (store instanceof ConfigStore.Discoverable discovery
                && store.capabilities().contains(Capability.DISCOVER)) {
            return discovery;
        }
        throw new MigrationException(UNSUPPORTED_CAPABILITY, "Nacos discovery is not implemented");
    }

    /**
     * Validates target-format content and publishes it unchanged, regardless of the configured
     * source format.
     */
    public PublishResult nacosPublish(
            String dataId, String content, String group, String namespace, String fileType) {
        Location location = nacosLocation(dataId, group, namespace);
        FileType type = FileType.parse(or(fileType, settings.to().nacos().fileType()));
        // Reject malformed target documents before login or any upstream write can occur.
        formats.validate(content, type);
        // Publication is deliberately independent of the configured source format.
        return writable(Provider.NACOS).write(new WriteRequest(location, type, content));
    }

    /**
     * Explicitly converts content, using configured from/to formats only as omitted-argument
     * defaults.
     */
    public ConversionResult convert(String content, String sourceType, String targetType) {
        return formats.convert(
                content,
                FileType.parse(or(sourceType, settings.from().apollo().fileType())),
                FileType.parse(or(targetType, settings.to().nacos().fileType())));
    }

    /** Resolves a declared read capability or reports an unsupported-capability failure. */
    public ConfigStore.Readable readable(Provider provider) {
        ConfigStore store = stores.get(provider);
        if (store instanceof ConfigStore.Readable reader
                && store.capabilities().contains(Capability.READ)) return reader;
        throw new MigrationException(UNSUPPORTED_CAPABILITY, provider + " read is not implemented");
    }

    /** Resolves a declared write capability without advertising reserved provider support. */
    public ConfigStore.Writable writable(Provider provider) {
        ConfigStore store = stores.get(provider);
        if (store instanceof ConfigStore.Writable writer
                && store.capabilities().contains(Capability.WRITE)) return writer;
        throw new MigrationException(
                UNSUPPORTED_CAPABILITY, provider + " write is not implemented");
    }

    /**
     * Builds a Nacos identity, allowing explicit public selection to override a configured
     * namespace.
     */
    private Location nacosLocation(String dataId, String group, String namespace) {
        return new Location(
                Provider.NACOS,
                null,
                null,
                nacosNamespace(namespace),
                nacosGroup(group),
                required(dataId, "dataId"));
    }

    private String nacosNamespace(String namespace) {
        String resolved = or(namespace, settings.to().nacos().namespace());
        return resolved.equals("public") ? "" : resolved;
    }

    private String nacosGroup(String group) {
        return required(or(group, settings.to().nacos().group()), "group");
    }

    /** Falls back only on omission; an empty string remains an explicit argument. */
    private static String or(String value, String fallback) {
        return value == null ? fallback : value;
    }
}
