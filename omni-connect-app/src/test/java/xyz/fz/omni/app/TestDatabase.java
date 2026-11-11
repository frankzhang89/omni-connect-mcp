package xyz.fz.omni.app;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import xyz.fz.omni.jdbc.*;

/** H2 is available only in tests, never in the release JAR. */
@TestConfiguration(proxyBeanMethods = false)
public class TestDatabase {
    @Bean
    DatabaseDialect h2Dialect() {
        return new PostgreSqlDialect() {
            public String name() {
                return "H2 test database";
            }

            public String urlPrefix() {
                return "jdbc:h2:";
            }

            public String driverClassName() {
                return "org.h2.Driver";
            }

            public boolean transactionalTruncate() {
                return false;
            }
        };
    }
}
