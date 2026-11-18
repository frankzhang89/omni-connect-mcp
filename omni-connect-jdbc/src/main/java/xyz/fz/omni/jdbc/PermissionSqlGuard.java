package xyz.fz.omni.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/** Identifies native permission commands while leaving database syntax and policy to JDBC. */
final class PermissionSqlGuard {
    static final String LITERAL = "<literal>";
    static final String IDENTIFIER = "<identifier>";
    private static final Pattern DOLLAR_QUOTE =
            Pattern.compile("\\$(?:[\\p{L}_][\\p{L}\\p{N}_]*)?\\$");

    private enum Dialect {
        MYSQL,
        POSTGRES,
        ORACLE,
        SQL_SERVER,
        STARROCKS;

        boolean mysqlQuotes() {
            return this == MYSQL || this == STARROCKS;
        }
    }

    private PermissionSqlGuard() {}

    static String validate(String sql, DatabaseDialect target) {
        if (sql == null || sql.isBlank() || sql.length() > 20_000 || sql.indexOf('\0') >= 0) {
            throw new Rejected("SQL must be nonempty, contain no NUL, and be at most 20000 characters.");
        }
        Dialect dialect = dialectOf(target);
        String body = sql.strip();
        checkCommand(scan(body, dialect, false, false), dialect);
        // A pooled connection may use a nondefault string mode. Statement boundaries must
        // remain safe under MySQL's ANSI_QUOTES/NO_BACKSLASH_ESCAPES and PostgreSQL's
        // standard_conforming_strings. Standard quote doubling works in all of these modes.
        if (body.indexOf('\\') >= 0) {
            if (dialect == Dialect.POSTGRES || dialect.mysqlQuotes()) {
                checkCommand(scan(body, dialect, true, false), dialect);
            }
            if (dialect.mysqlQuotes()) {
                checkCommand(scan(body, dialect, true, true), dialect);
            }
        }
        return body.endsWith(";") ? body.substring(0, body.length() - 1).stripTrailing() : body;
    }

    private static Dialect dialectOf(DatabaseDialect target) {
        return switch (target) {
            case MySqlDialect ignored -> Dialect.MYSQL;
            case PostgreSqlDialect ignored -> Dialect.POSTGRES;
            case OracleDialect ignored -> Dialect.ORACLE;
            case SqlServerDialect ignored -> Dialect.SQL_SERVER;
            case StarRocksDialect ignored -> Dialect.STARROCKS;
            default -> throw new Rejected("Permission management is unavailable for this JDBC dialect.");
        };
    }

    private static void checkCommand(List<String> tokens, Dialect dialect) {
        if (tokens.size() < 3) {
            throw rejected();
        }
        if (dialect == Dialect.SQL_SERVER) {
            new SqlServerPermissionSyntax(tokens).validate();
            return;
        }
        String command = tokens.get(0);
        String object = tokens.get(1);
        switch (command) {
            case "GRANT", "REVOKE" -> {
                if (dialect == Dialect.MYSQL
                        && command.equals("GRANT")
                        && hasMysqlGrantAuthentication(tokens)) {
                    throw rejected();
                }
            }
            case "CREATE", "DROP" -> {
                if (!object.equals("USER") && !object.equals("ROLE")) {
                    throw rejected();
                }
            }
            case "ALTER" -> {
                if (!object.equals("USER")
                        && !(object.equals("ROLE")
                                && (dialect == Dialect.POSTGRES || dialect == Dialect.ORACLE))) {
                    throw rejected();
                }
                if (dialect == Dialect.POSTGRES) {
                    checkPostgresAlter(tokens);
                }
            }
            case "RENAME" -> {
                if (dialect != Dialect.MYSQL || !object.equals("USER")) {
                    throw rejected();
                }
            }
            case "SET" -> {
                boolean defaultRole =
                        object.equals("DEFAULT")
                                && tokens.size() > 3
                                && tokens.get(2).equals("ROLE");
                if (!dialect.mysqlQuotes() || !(object.equals("PASSWORD") || defaultRole)) {
                    throw rejected();
                }
            }
            default -> throw rejected();
        }
    }

    private static boolean hasMysqlGrantAuthentication(List<String> tokens) {
        for (int i = 1; i + 1 < tokens.size(); i++) {
            if (tokens.get(i).equals("IDENTIFIED")
                    && (tokens.get(i + 1).equals("BY") || tokens.get(i + 1).equals("WITH"))) {
                return true;
            }
        }
        return false;
    }

    private static void checkPostgresAlter(List<String> tokens) {
        // ALTER ROLE/USER can set arbitrary session variables. Allow only the persisted
        // role selection, optionally scoped to a database, as part of account management.
        int clause = 3;
        if (clause < tokens.size() && tokens.get(clause).equals("IN")) {
            if (tokens.size() < 7 || !tokens.get(clause + 1).equals("DATABASE")) {
                throw rejected();
            }
            clause += 3;
        }
        if (clause == tokens.size()) {
            return;
        }
        String operation = tokens.get(clause);
        if (operation.equals("SET") || operation.equals("RESET")) {
            if (clause + 1 >= tokens.size() || !tokens.get(clause + 1).equals("ROLE")) {
                throw rejected();
            }
        }
    }

    private static List<String> scan(
            String sql, Dialect dialect, boolean singleBackslash, boolean doubleBackslash) {
        var tokens = new ArrayList<String>();
        for (int pos = 0; pos < sql.length(); ) {
            char ch = sql.charAt(pos);
            if (Character.isWhitespace(ch)) {
                pos++;
            } else if (sql.startsWith("--", pos)
                    || sql.startsWith("/*", pos)
                    || (ch == '#' && dialect.mysqlQuotes())) {
                throw rejected();
            } else if (ch == ';') {
                if (pos != sql.length() - 1) {
                    throw rejected();
                }
                break;
            } else if (ch == '\''
                    || ch == '"'
                    || (ch == '`' && dialect.mysqlQuotes())
                    || (ch == '[' && dialect == Dialect.SQL_SERVER)) {
                char close = ch == '[' ? ']' : ch;
                boolean backslash = ch == '\'' ? singleBackslash : ch == '"' && doubleBackslash;
                pos = quotedEnd(sql, pos + 1, close, backslash);
                tokens.add(ch == '\'' ? LITERAL : IDENTIFIER);
            } else if (ch == '$' && dialect == Dialect.POSTGRES) {
                pos = dollarQuotedEnd(sql, pos);
                tokens.add(LITERAL);
            } else if (isWordPart(ch, dialect)) {
                int end = pos + 1;
                while (end < sql.length() && isWordPart(sql.charAt(end), dialect)) {
                    end++;
                }
                String word = sql.substring(pos, end).toUpperCase(Locale.ROOT);
                if (end < sql.length() && sql.charAt(end) == '\'') {
                    if ((dialect == Dialect.POSTGRES && word.equals("E"))
                            || (dialect == Dialect.SQL_SERVER && word.equals("N"))) {
                        pos = quotedEnd(sql, end + 1, '\'', dialect == Dialect.POSTGRES);
                        tokens.add(LITERAL);
                        continue;
                    }
                    if (dialect == Dialect.ORACLE && (word.equals("Q") || word.equals("NQ"))) {
                        pos = oracleQuotedEnd(sql, end + 1);
                        tokens.add(LITERAL);
                        continue;
                    }
                }
                if (dialect == Dialect.POSTGRES && word.equals("U") && end + 1 < sql.length()
                        && sql.charAt(end) == '&') {
                    char quote = sql.charAt(end + 1);
                    if (quote == '\'' || quote == '"') {
                        pos = postgresUnicodeQuoteEnd(sql, end + 2, quote);
                        tokens.add(quote == '"' ? IDENTIFIER : LITERAL);
                        continue;
                    }
                }
                tokens.add(word);
                pos = end;
            } else if (sql.startsWith("::", pos)) {
                tokens.add("::");
                pos += 2;
            } else {
                tokens.add(String.valueOf(ch));
                pos++;
            }
        }
        return tokens;
    }

    static boolean wordStart(char ch) {
        return Character.isLetter(ch) || ch == '_';
    }

    private static boolean isWordPart(char ch, Dialect dialect) {
        return wordStart(ch)
                || Character.isDigit(ch)
                || ch == '$'
                || (dialect == Dialect.SQL_SERVER && (ch == '#' || ch == '@'));
    }

    private static int quotedEnd(String sql, int start, char close, boolean backslash) {
        for (int pos = start; pos < sql.length(); pos++) {
            char ch = sql.charAt(pos);
            if (ch == '\\' && backslash) {
                pos++;
            } else if (ch == close) {
                if (pos + 1 == sql.length() || sql.charAt(pos + 1) != close) {
                    return pos + 1;
                }
                pos++;
            }
        }
        throw rejected();
    }

    private static int dollarQuotedEnd(String sql, int start) {
        var tag = DOLLAR_QUOTE.matcher(sql).region(start, sql.length());
        if (!tag.lookingAt()) {
            throw rejected();
        }
        String delimiter = tag.group();
        int end = sql.indexOf(delimiter, tag.end());
        if (end < 0) {
            throw rejected();
        }
        return end + delimiter.length();
    }

    private static int postgresUnicodeQuoteEnd(String sql, int start, char quote) {
        int end = quotedEnd(sql, start, quote, false);
        int escapeClause = skipWhitespace(sql, end);
        String keyword = "UESCAPE";
        int afterKeyword = escapeClause + keyword.length();
        if (!sql.regionMatches(true, escapeClause, keyword, 0, keyword.length())
                || (afterKeyword < sql.length()
                        && isWordPart(sql.charAt(afterKeyword), Dialect.POSTGRES))) {
            return end;
        }
        int value = skipWhitespace(sql, afterKeyword);
        if (value == sql.length() || sql.charAt(value) != '\'') {
            throw rejected();
        }
        return quotedEnd(sql, value + 1, '\'', false);
    }

    private static int skipWhitespace(String sql, int pos) {
        while (pos < sql.length() && Character.isWhitespace(sql.charAt(pos))) {
            pos++;
        }
        return pos;
    }

    private static int oracleQuotedEnd(String sql, int start) {
        if (start >= sql.length()) {
            throw rejected();
        }
        char delimiter = sql.charAt(start);
        char close = switch (delimiter) {
            case '[' -> ']';
            case '{' -> '}';
            case '(' -> ')';
            case '<' -> '>';
            default -> delimiter;
        };
        int end = sql.indexOf(close + "'", start + 1);
        if (end < 0) {
            throw rejected();
        }
        return end + 2;
    }

    static Rejected rejected() {
        return new Rejected("Expected a single supported, comment-free permission statement.");
    }

    static final class Rejected extends IllegalArgumentException {
        Rejected(String message) {
            super(message);
        }
    }
}
