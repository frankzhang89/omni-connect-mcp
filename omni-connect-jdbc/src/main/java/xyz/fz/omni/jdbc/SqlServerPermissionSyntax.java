package xyz.fz.omni.jdbc;

import static xyz.fz.omni.jdbc.PermissionSqlGuard.IDENTIFIER;
import static xyz.fz.omni.jdbc.PermissionSqlGuard.LITERAL;
import static xyz.fz.omni.jdbc.PermissionSqlGuard.rejected;
import static xyz.fz.omni.jdbc.PermissionSqlGuard.wordStart;

import java.util.List;
import java.util.Set;

/**
 * Reads the complete supported T-SQL permission statement. SQL Server batches allow consecutive
 * commands without terminators, so inspecting the first keyword or counting semicolons is
 * insufficient. SQL Server itself checks permission names and the applicability of each clause.
 */
final class SqlServerPermissionSyntax {
    private static final Set<String> PRIVILEGE_BOUNDARIES = Set.of("ON", "TO", "FROM");

    private final List<String> tokens;
    private int position;

    SqlServerPermissionSyntax(List<String> tokens) {
        this.tokens = tokens;
    }

    void validate() {
        switch (next()) {
            case "GRANT" -> privileges(true);
            case "REVOKE" -> privileges(false);
            case "CREATE" -> create();
            case "DROP" -> drop();
            case "ALTER" -> alter();
            default -> throw rejected();
        }
        if (position != tokens.size()) {
            throw rejected();
        }
    }

    private void privileges(boolean grant) {
        if (!grant && take("GRANT")) {
            require("OPTION");
            require("FOR");
        }
        do {
            int start = position;
            while (isWord(peek()) && !PRIVILEGE_BOUNDARIES.contains(peek())) {
                position++;
            }
            if (start == position) {
                throw rejected();
            }
            columns();
        } while (take(","));

        if (take("ON")) {
            int separator = position;
            while (isWord(tokenAt(separator)) && !PRIVILEGE_BOUNDARIES.contains(tokenAt(separator))) {
                separator++;
            }
            if (separator > position && tokenAt(separator).equals("::")) {
                position = separator + 1;
            }
            name();
            if (take(".")) {
                name();
            }
            columns();
        }
        if (grant || !take("FROM")) {
            require("TO");
        }
        names();
        if (grant && take("WITH")) {
            require("GRANT");
            require("OPTION");
        }
        if (!grant) {
            take("CASCADE");
        }
        if (take("AS")) {
            name();
        }
    }

    private void columns() {
        if (take("(")) {
            names();
            require(")");
        }
    }

    private void create() {
        String type = next();
        switch (type) {
            case "LOGIN" -> {
                name();
                if (take("FROM")) {
                    switch (next()) {
                        case "WINDOWS" -> { }
                        case "CERTIFICATE" -> name();
                        case "ASYMMETRIC" -> {
                            require("KEY");
                            name();
                        }
                        case "EXTERNAL" -> require("PROVIDER");
                        default -> throw rejected();
                    }
                    if (take("WITH")) {
                        loginOptions();
                    }
                } else {
                    require("WITH");
                    loginOptions();
                }
            }
            case "USER" -> {
                name();
                if (take("FOR") || take("FROM")) {
                    switch (next()) {
                        case "LOGIN", "CERTIFICATE" -> name();
                        case "ASYMMETRIC" -> {
                            require("KEY");
                            name();
                        }
                        case "EXTERNAL" -> require("PROVIDER");
                        default -> throw rejected();
                    }
                } else if (take("WITHOUT")) {
                    require("LOGIN");
                }
                if (take("WITH")) {
                    userOptions();
                }
            }
            case "SERVER", "ROLE" -> {
                if (type.equals("SERVER")) {
                    require("ROLE");
                }
                name();
                if (take("AUTHORIZATION")) {
                    name();
                }
            }
            default -> throw rejected();
        }
    }

    private void drop() {
        switch (next()) {
            case "LOGIN" -> name();
            case "SERVER" -> {
                require("ROLE");
                name();
            }
            case "USER", "ROLE" -> {
                if (take("IF")) {
                    require("EXISTS");
                }
                name();
            }
            default -> throw rejected();
        }
    }

    private void alter() {
        String type = next();
        switch (type) {
            case "LOGIN" -> {
                name();
                if (!take("ENABLE") && !take("DISABLE")) {
                    require("WITH");
                    loginOptions();
                }
            }
            case "USER" -> {
                name();
                require("WITH");
                userOptions();
            }
            case "SERVER", "ROLE" -> {
                if (type.equals("SERVER")) {
                    require("ROLE");
                }
                name();
                if (!take("ADD")) {
                    require("DROP");
                }
                require("MEMBER");
                name();
            }
            default -> throw rejected();
        }
    }

    private void loginOptions() {
        do {
            String option = next();
            require("=");
            switch (option) {
                case "PASSWORD" -> {
                    if (!take(LITERAL) && !take(IDENTIFIER)) {
                        if (!next().matches("0X[0-9A-F]+")) {
                            throw rejected();
                        }
                        require("HASHED");
                    }
                    if (take("OLD_PASSWORD")) {
                        require("=");
                        string();
                    } else {
                        take("MUST_CHANGE");
                        take("UNLOCK");
                    }
                }
                case "CHECK_POLICY", "CHECK_EXPIRATION" -> onOff();
                case "DEFAULT_DATABASE", "DEFAULT_LANGUAGE", "NAME", "CREDENTIAL" -> name();
                case "OBJECT_ID" -> string();
                case "SID" -> {
                    if (!next().matches("0X[0-9A-F]+")) {
                        throw rejected();
                    }
                }
                default -> throw rejected();
            }
        } while (take(","));
    }

    private void userOptions() {
        do {
            String option = next();
            require("=");
            switch (option) {
                case "PASSWORD" -> {
                    string();
                    if (take("OLD_PASSWORD")) {
                        require("=");
                        string();
                    }
                }
                case "OLD_PASSWORD", "OBJECT_ID" -> string();
                case "DEFAULT_SCHEMA", "DEFAULT_LANGUAGE", "NAME", "LOGIN" -> name();
                case "ALLOW_ENCRYPTED_VALUE_MODIFICATIONS" -> onOff();
                default -> throw rejected();
            }
        } while (take(","));
    }

    private void onOff() {
        if (!take("ON")) {
            require("OFF");
        }
    }

    private void string() {
        if (!take(LITERAL)) {
            require(IDENTIFIER);
        }
    }

    private void names() {
        do {
            name();
        } while (take(","));
    }

    private void name() {
        if (!take(IDENTIFIER) && !isWord(next())) {
            throw rejected();
        }
    }

    private static boolean isWord(String token) {
        return !token.isEmpty() && wordStart(token.charAt(0));
    }

    private void require(String token) {
        if (!take(token)) {
            throw rejected();
        }
    }

    private boolean take(String token) {
        if (!peek().equals(token)) {
            return false;
        }
        position++;
        return true;
    }

    private String next() {
        if (position == tokens.size()) {
            throw rejected();
        }
        return tokens.get(position++);
    }

    private String peek() {
        return tokenAt(position);
    }

    private String tokenAt(int index) {
        return index < tokens.size() ? tokens.get(index) : "";
    }
}
