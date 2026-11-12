package xyz.fz.omni.config.adapter;

import static xyz.fz.omni.config.core.MigrationException.Code.*;

import xyz.fz.omni.config.core.MigrationException;

import java.util.regex.Pattern;

/** A connection generation's detected version and immutable HTTP API selection. */
record NacosServerVersion(String value, int major, int minor, int patch, Api api) {
    private static final Pattern VERSION =
            Pattern.compile("([0-9]+)\\.([0-9]+)\\.([0-9]+)(?:[-+.][A-Za-z0-9][A-Za-z0-9.+_-]*)?");

    enum Api {
        LEGACY("/v1/console/namespaces", 200),
        V2("/v2/console/namespace/list", 0),
        V3("/v3/admin/core/namespace/list", 0);

        final String namespaces;
        final int successCode;

        Api(String namespaces, int successCode) {
            this.namespaces = namespaces;
            this.successCode = successCode;
        }

        String configPath() {
            return this == V3 ? "/v3/admin/cs/config" : "/v1/cs/configs";
        }

        String listPath() {
            return this == V3 ? "/v3/admin/cs/config/list" : configPath();
        }

        String loginPath() {
            return this == V3 ? "/v3/auth/user/login" : "/v1/auth/login";
        }
    }

    static NacosServerVersion parse(String value) {
        var matcher = VERSION.matcher(value);
        if (!matcher.matches()) throw invalid();
        int major;
        int minor;
        int patch;
        try {
            major = Integer.parseInt(matcher.group(1));
            minor = Integer.parseInt(matcher.group(2));
            patch = Integer.parseInt(matcher.group(3));
        } catch (NumberFormatException e) {
            throw invalid();
        }
        Api api =
                switch (major) {
                    case 1 -> Api.LEGACY;
                    case 2 -> minor >= 2 ? Api.V2 : Api.LEGACY;
                    case 3 -> Api.V3;
                    default ->
                            throw new MigrationException(
                                    UNSUPPORTED_CAPABILITY,
                                    "Unsupported Nacos server major version");
                };
        return new NacosServerVersion(value, major, minor, patch, api);
    }

    private static MigrationException invalid() {
        return new MigrationException(UPSTREAM_ERROR, "Nacos returned an invalid server version");
    }
}
