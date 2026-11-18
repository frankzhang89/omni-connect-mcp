package xyz.fz.omni.jdbc;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

class PermissionSqlGuardTest {
    @ParameterizedTest
    @MethodSource("allowed")
    void preservesNativeSqlAndStripsTheClientTerminator(DatabaseDialect dialect, String sql) {
        assertEquals(sql, PermissionSqlGuard.validate(" \n" + sql + "; \r\n", dialect));
    }

    static Stream<Arguments> allowed() {
        return Stream.of(
                        cases(
                                new MySqlDialect(),
                                "GRANT SELECT ON db2.xx TO 'reader'@'localhost'",
                                "revoke select on db2.xx from 'reader'@'localhost'",
                                "GRANT SELECT ON `db2`.* TO 'reader'@'%'",
                                "GRANT 'app_reader' TO 'reader'@'localhost'",
                                "REVOKE 'app_reader' FROM 'reader'@'localhost'",
                                "GRANT SELECT ON *.* TO 'identified'@'127.0.0.1'",
                                "GRANT SELECT ON db2.identified TO reader",
                                "CREATE USER 'app'@'192.0.2.1' IDENTIFIED BY 'p;--#/*a''b'",
                                "ALTER USER 'app'@'192.0.2.1' ACCOUNT LOCK",
                                "ALTER USER 'app'@'192.0.2.1' ACCOUNT UNLOCK",
                                "SET PASSWORD FOR 'app'@'192.0.2.1' = 'changed'",
                                "RENAME USER 'app'@'192.0.2.1' TO 'app'@'192.0.2.2'",
                                "DROP USER 'app'@'192.0.2.2'",
                                "CREATE ROLE 'app_reader'",
                                "SET DEFAULT ROLE 'app_reader' TO 'app'@'192.0.2.2'",
                                "DROP ROLE 'app_reader'"),
                        cases(
                                new PostgreSqlDialect(),
                                "GRANT SELECT ON TABLE public.orders TO reader",
                                "REVOKE SELECT ON TABLE public.orders FROM reader",
                                "GRANT app_reader TO reader WITH INHERIT TRUE",
                                "REVOKE app_reader FROM reader",
                                "CREATE USER app PASSWORD $pw$p;--#/*a'b$pw$",
                                "ALTER USER app WITH PASSWORD E'p\\\\q\\'r;--#'",
                                "ALTER ROLE app WITH NOLOGIN VALID UNTIL '2027-11-30'",
                                "ALTER ROLE app LOGIN",
                                "ALTER ROLE app IN DATABASE appdb SET role TO app_reader",
                                "ALTER ROLE app RESET role",
                                "ALTER ROLE U&\"!0061pp\" UESCAPE '!' SET role TO app_reader",
                                "DROP USER app",
                                "CREATE ROLE app_reader",
                                "DROP ROLE app_reader"),
                        cases(
                                new OracleDialect(),
                                "GRANT SELECT ON hr.orders TO reader",
                                "REVOKE SELECT ON hr.orders FROM reader",
                                "GRANT app_reader TO reader",
                                "REVOKE app_reader FROM reader",
                                "CREATE USER app IDENTIFIED BY \"p;--#/*a'b\"",
                                "ALTER USER app IDENTIFIED BY \"different\"",
                                "ALTER USER app ACCOUNT LOCK",
                                "ALTER USER app ACCOUNT UNLOCK",
                                "ALTER USER app DEFAULT ROLE app_reader",
                                "DROP USER app",
                                "CREATE ROLE app_reader",
                                "DROP ROLE app_reader"),
                        cases(
                                new SqlServerDialect(),
                                "GRANT SELECT ON OBJECT::[dbo].[orders] TO [reader]",
                                "GRANT VIEW SERVER STATE TO [reader]",
                                "GRANT SELECT ([order id]), UPDATE ON [dbo].[orders] TO [reader],"
                                        + " [ops] WITH GRANT OPTION AS [dbo]",
                                "REVOKE GRANT OPTION FOR SELECT ON SCHEMA::dbo FROM [reader]"
                                        + " CASCADE AS dbo",
                                "REVOKE IMPERSONATE ANY LOGIN FROM [reader]",
                                "CREATE LOGIN [app] WITH PASSWORD = 'p;--#/*a''b', CHECK_POLICY = ON",
                                "CREATE LOGIN [DOMAIN\\app] FROM WINDOWS WITH DEFAULT_DATABASE = demo",
                                "CREATE LOGIN [Cloud Login] FROM EXTERNAL PROVIDER WITH OBJECT_ID = 'id'",
                                "ALTER LOGIN [app] WITH PASSWORD = N'changed' OLD_PASSWORD = 'old'",
                                "ALTER LOGIN [app] DISABLE",
                                "ALTER LOGIN [app] ENABLE",
                                "DROP LOGIN [app]",
                                "CREATE USER [app] FOR LOGIN [app] WITH DEFAULT_SCHEMA = dbo",
                                "CREATE USER [app] WITH PASSWORD = 'contained'",
                                "ALTER USER [app] WITH PASSWORD = 'changed', OLD_PASSWORD = 'old'",
                                "DROP USER IF EXISTS [app]",
                                "CREATE ROLE [app_reader] AUTHORIZATION [dbo]",
                                "ALTER ROLE [app_reader] ADD MEMBER [app]]test]",
                                "ALTER ROLE [app_reader] DROP MEMBER [app]]test]",
                                "DROP ROLE [app_reader]",
                                "CREATE SERVER ROLE [app_reader]",
                                "ALTER SERVER ROLE [app_reader] ADD MEMBER [app]",
                                "ALTER SERVER ROLE [app_reader] DROP MEMBER [app]",
                                "DROP SERVER ROLE [app_reader]"),
                        cases(
                                new StarRocksDialect(),
                                "GRANT SELECT ON TABLE db2.xx TO USER 'reader'@'192.0.2.1'",
                                "REVOKE SELECT ON TABLE db2.xx FROM USER 'reader'@'192.0.2.1'",
                                "GRANT app_reader TO USER 'reader'@'192.0.2.1'",
                                "REVOKE app_reader FROM USER 'reader'@'192.0.2.1'",
                                "CREATE USER 'app'@'192.0.2.1' IDENTIFIED BY 'p;--#/*a''b'",
                                "ALTER USER 'app'@'192.0.2.1' IDENTIFIED BY 'changed'",
                                "SET PASSWORD FOR 'app'@['client.example'] = PASSWORD('changed')",
                                "ALTER USER 'app'@'192.0.2.1' DEFAULT ROLE app_reader",
                                "SET DEFAULT ROLE app_reader TO 'app'@'192.0.2.1'",
                                "DROP USER 'app'@'192.0.2.1'",
                                "CREATE ROLE app_reader",
                                "DROP ROLE app_reader"))
                .flatMap(stream -> stream);
    }

    @ParameterizedTest
    @MethodSource("rejected")
    void rejectsOutOfScopeOrAmbiguousSql(DatabaseDialect dialect, String sql) {
        var error =
                assertThrows(
                        PermissionSqlGuard.Rejected.class,
                        () -> PermissionSqlGuard.validate(sql, dialect));
        assertFalse(error.getMessage().contains("password-marker"));
    }

    static Stream<Arguments> rejected() {
        return Stream.of(
                        cases(
                                new MySqlDialect(),
                                null,
                                "",
                                "x".repeat(20_001),
                                "USE db2",
                                "SET ROLE ALL",
                                "CREATE TABLE test(id INT)",
                                "DROP DATABASE demo",
                                "UPDATE mysql.user SET Host = 'example'",
                                "GRANT SELECT ON a TO b; REVOKE SELECT ON a FROM c",
                                "GRANT SELECT ON a TO b;;",
                                "GRANT SELECT ON a TO b#comment",
                                "GRANT SELECT ON a TO b /* comment */",
                                "GRANT SELECT ON a TO b /*!50000 WITH GRANT OPTION */",
                                "GRANT SELECT ON a TO b IDENTIFIED BY 'password-marker'",
                                "CREATE USER u IDENTIFIED BY 'password-marker\\'; DROP TABLE t; -- '",
                                "ALTER USER u IDENTIFIED BY 'open",
                                "ALTER USER u IDENTIFIED BY 'x\0y'"),
                        cases(
                                new PostgreSqlDialect(),
                                "ALTER ROLE app SET search_path TO test",
                                "ALTER USER app RESET ALL",
                                "ALTER ROLE app IN DATABASE demo SET session_preload_libraries TO 'x'",
                                "ALTER ROLE U&\"!0061pp\" UESCAPE '!' SET search_path TO test",
                                "ALTER ROLE app IN DATABASE U&\"demo\" SET search_path TO test",
                                "ALTER USER app PASSWORD $pw$pass$pw$; SELECT 1",
                                "ALTER USER app PASSWORD $pw$open"),
                        cases(
                                new OracleDialect(),
                                "BEGIN EXECUTE IMMEDIATE 'CREATE USER x'; END;",
                                "ALTER SYSTEM SET log_archive_dest = 'x'",
                                "CREATE PACKAGE account_tools AS END;"),
                        cases(
                                new SqlServerDialect(),
                                "GRANT SELECT ON dbo.orders TO reader\nDROP TABLE dbo.orders",
                                "GRANT SELECT ON dbo.orders TO reader WITH GRANT OPTION\nSELECT 1",
                                "REVOKE SELECT ON dbo.orders FROM reader\nUSE master",
                                "CREATE LOGIN app WITH PASSWORD = 'password-marker'\nEXEC other_proc",
                                "CREATE USER app WITHOUT LOGIN\nSELECT 1",
                                "ALTER LOGIN app WITH PASSWORD = N'password-marker'\nSELECT 1",
                                "ALTER LOGIN app ENABLE\nDROP LOGIN another",
                                "DROP USER app\nGRANT SELECT TO somebody",
                                "DROP USER app; DROP LOGIN app",
                                "ALTER ROLE app_reader ADD MEMBER app\nSELECT 1",
                                "ALTER ROLE app_reader WITH NAME = new_role",
                                "EXEC sp_set_firewall_rule",
                                "SET PASSWORD = 'password-marker'",
                                "ALTER LOGIN app WITH PASSWORD = 'pass' -- hidden"),
                        cases(
                                new StarRocksDialect(),
                                "RENAME USER 'app'@'192.0.2.1' TO 'app'@'192.0.2.2'",
                                "SET ROLE app_reader",
                                "CREATE USER 'app'@'192.0.2.1' IDENTIFIED BY 'x'; DROP USER 'app'"))
                .flatMap(stream -> stream);
    }

    private static Stream<Arguments> cases(DatabaseDialect dialect, String... statements) {
        return Arrays.stream(statements).map(sql -> arguments(dialect, sql));
    }
}
