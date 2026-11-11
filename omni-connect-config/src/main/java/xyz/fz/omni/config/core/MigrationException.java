package xyz.fz.omni.config.core;

public final class MigrationException extends RuntimeException {
    public enum Code {
        INVALID_ARGUMENT,
        INVALID_FORMAT,
        CONVERSION_CONFLICT,
        NOT_FOUND,
        AUTHENTICATION_FAILED,
        UPSTREAM_ERROR,
        NETWORK_ERROR,
        TIMEOUT,
        WRITE_RESULT_UNKNOWN,
        PUBLISH_FAILED,
        UNSUPPORTED_CAPABILITY,
        CONCURRENT_MODIFICATION,
        VERIFICATION_FAILED
    }

    private final Code code;
    private final java.util.Map<String, Object> details;

    /**
     * Creates a tool-facing failure whose message must not contain credentials or configuration
     * snippets.
     */
    public MigrationException(Code code, String safeMessage) {
        this(code, safeMessage, java.util.Map.of());
    }

    public MigrationException(
            Code code, String safeMessage, java.util.Map<String, Object> details) {
        super(code + ": " + safeMessage);
        this.code = code;
        this.details =
                java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(details));
    }

    public java.util.Map<String, Object> details() {
        return details;
    }

    /** Returns the stable error code used by MCP clients to select a recovery action. */
    public Code code() {
        return code;
    }
}
