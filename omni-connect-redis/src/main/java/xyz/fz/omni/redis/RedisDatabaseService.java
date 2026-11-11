package xyz.fz.omni.redis;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds the current Redis database. Spring Data Redis rejects SELECT on a shared Lettuce
 * connection, so every database (including the default one from {@code
 * omni.redis.connection.database}) gets its own lazily created connection factory. Tool limits such
 * as {@code omni.redis.limits.max-items} are optional properties with sensible defaults.
 */
public class RedisDatabaseService implements DisposableBean {

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final int defaultDb;
    private final int maxDatabases;
    // One lazily created factory/template per database index.
    private final ConcurrentHashMap<Integer, LettuceConnectionFactory> factories =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, StringRedisTemplate> templates =
            new ConcurrentHashMap<>();

    // Global selection shared by all clients. Volatile publishes a successful choice
    // to later calls; it does not serialize concurrent selectDatabase() validations.
    private volatile int currentDb;

    public RedisDatabaseService(
            @Value("${omni.redis.connection.database:0}") int defaultDb,
            @Value("${omni.redis.connection.host:localhost}") String host,
            @Value("${omni.redis.connection.port:6379}") int port,
            @Value("${omni.redis.connection.username:}") String username,
            @Value("${omni.redis.connection.password:}") String password,
            @Value("${omni.redis.limits.max-databases:16}") int maxDatabases) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.maxDatabases = Math.max(1, maxDatabases);
        if (defaultDb < 0 || defaultDb >= this.maxDatabases) {
            throw new IllegalArgumentException(
                    "omni.redis.connection.database must be between 0 and "
                            + (this.maxDatabases - 1)
                            + ".");
        }
        this.defaultDb = defaultDb;
        this.currentDb = defaultDb;
    }

    public int currentDatabase() {
        return currentDb;
    }

    /**
     * Captures the current index once and returns its database-specific template. A service keeps
     * this template for its operation, even if selection changes later.
     */
    public StringRedisTemplate currentTemplate() {
        return templateFor(currentDb);
    }

    /**
     * Switches the Redis logical database on the same host, independently of JDBC. Previously
     * created templates stay cached for reuse and close during shutdown; unlike JDBC targets, they
     * are not retired after each selection.
     */
    public Map<String, Object> selectDatabase(Integer db) {
        if (db == null || db < 0 || db >= maxDatabases) {
            throw new IllegalArgumentException(
                    "The database index must be between 0 and " + (maxDatabases - 1) + ".");
        }
        // Build (or reuse) the template for the target database.
        StringRedisTemplate target = templateFor(db);
        // Verify reachability and credentials with PING before switching, so a
        // failed switch leaves the current database untouched.
        try {
            target.execute((RedisCallback<String>) connection -> connection.ping());
        } catch (RuntimeException ex) {
            throw new IllegalStateException(
                    "Failed to switch to database " + db + ": " + ex.getMessage());
        }
        // Publish only after PING succeeds. Concurrent selections can validate in
        // parallel; whichever assignment happens last determines the global selection.
        currentDb = db;
        return Map.of("database", db);
    }

    private StringRedisTemplate templateFor(int db) {
        // computeIfAbsent guarantees a single factory per database index.
        return templates.computeIfAbsent(db, this::createTemplate);
    }

    private StringRedisTemplate createTemplate(int db) {
        // Bind a separate connection factory to this index instead of issuing SELECT
        // on a shared connection that other operations may already be using.
        RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration(host, port);
        configuration.setDatabase(db);
        // Apply the shared credentials only when they are actually set; an
        // empty password would otherwise trigger a failing AUTH command.
        if (username != null && !username.isBlank()) {
            configuration.setUsername(username);
        }
        if (password != null && !password.isBlank()) {
            configuration.setPassword(RedisPassword.of(password));
        }
        LettuceConnectionFactory factory = new LettuceConnectionFactory(configuration);
        factory.afterPropertiesSet();
        // Track the factory so destroy() can shut it down with the application.
        factories.put(db, factory);
        StringRedisTemplate template = new StringRedisTemplate(factory);
        template.afterPropertiesSet();
        return template;
    }

    @Override
    public void destroy() {
        // Close every dynamically created factory; the auto-configured one is
        // managed by Spring itself.
        factories.values().forEach(LettuceConnectionFactory::destroy);
        factories.clear();
        templates.clear();
    }
}
