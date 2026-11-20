package xyz.fz.omni.config.adapter;

import static xyz.fz.omni.config.core.MigrationException.Code.*;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.util.HtmlUtils;

import xyz.fz.omni.config.core.MigrationException;

import java.io.IOException;
import java.net.CookieManager;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/** One tool call's legacy Portal login session. */
final class ApolloPortalSession {
    private static final Pattern INPUT =
            Pattern.compile(
                    "<input\\b(?:[^>\"']++|\"[^\"]*\"|'[^']*')*+>", Pattern.CASE_INSENSITIVE);
    private static final Pattern ATTRIBUTE =
            Pattern.compile("\\s+([\\w:-]+)\\s*=\\s*(?:\"([^\"]*)\"|'([^']*)'|([^\\s>]+))");

    private final HttpTransport http;
    private final URI signIn;
    private final CookieManager cookies = new CookieManager();

    ApolloPortalSession(HttpTransport http, String portal) {
        this.http = http;
        signIn = URI.create(portal + "/signin");
    }

    void login(String username, String password) {
        var page = http.get(signIn, new HttpHeaders());
        http.requireSuccess(page);
        var fields = loginFields(page.body());
        if (!fields.containsKey("username") || !fields.containsKey("password")) {
            throw new MigrationException(UPSTREAM_ERROR, "Apollo Portal sign-in form is unsupported");
        }
        rememberCookies(signIn, page);
        var form = new LinkedMultiValueMap<String, String>();
        form.add("username", username);
        form.add("password", password);
        if (fields.containsKey("_csrf")) form.add("_csrf", fields.get("_csrf"));

        var login = http.post(signIn, cookieHeaders(signIn), form, false);
        if (login.status() == 302 || login.status() == 303) {
            String path = redirectPath(login, signIn);
            if (path == null)
                throw new MigrationException(UPSTREAM_ERROR, "Apollo Portal returned an unsupported redirect");
            if (signIn.getPath().equals(path)) throw authenticationFailed();
        } else {
            http.requireSuccess(login);
            if (isLoginPage(login)) throw authenticationFailed();
        }
        rememberCookies(signIn, login);
    }

    HttpTransport.Response get(URI uri) {
        requirePortal(uri);
        var response = http.get(uri, cookieHeaders(uri));
        checkSession(uri, response);
        rememberCookies(uri, response);
        return response;
    }

    HttpTransport.Response write(HttpMethod method, URI uri, Map<String, ?> body) {
        requirePortal(uri);
        var headers = cookieHeaders(uri);
        cookies.getCookieStore().get(uri).stream()
                .filter(cookie -> "XSRF-TOKEN".equals(cookie.getName()) && !cookie.hasExpired())
                .findFirst()
                .ifPresent(cookie -> headers.set("X-XSRF-TOKEN", cookie.getValue()));
        var response = http.writeJson(method, uri, headers, body);
        checkSession(uri, response);
        if (response.status() >= 200 && response.status() < 300) {
            try {
                rememberCookies(uri, response);
            } catch (MigrationException ignored) {
                throw new MigrationException(
                        WRITE_RESULT_UNKNOWN, "Apollo write response has an invalid session cookie");
            }
        }
        return response;
    }

    private void checkSession(URI uri, HttpTransport.Response response) {
        if ((response.status() == 302 || response.status() == 303)
                && signIn.getPath().equals(redirectPath(response, uri))) {
            throw authenticationFailed();
        }
        if (isLoginPage(response)) throw authenticationFailed();
    }

    private String redirectPath(HttpTransport.Response response, URI request) {
        String location = response.headers().getFirst(HttpHeaders.LOCATION);
        if (location != null) {
            try {
                URI target = request.resolve(location);
                if (sameOrigin(target)) return target.getPath();
            } catch (IllegalArgumentException ignored) {
                // Do not expose a malformed redirect or follow it.
            }
        }
        return null;
    }

    private boolean sameOrigin(URI target) {
        return signIn.getScheme().equalsIgnoreCase(target.getScheme())
                && signIn.getRawAuthority().equalsIgnoreCase(target.getRawAuthority());
    }

    private void requirePortal(URI target) {
        if (!sameOrigin(target)) {
            throw new MigrationException(UPSTREAM_ERROR, "Apollo request is outside the Portal origin");
        }
    }

    private void rememberCookies(URI uri, HttpTransport.Response response) {
        var received = response.headers().get(HttpHeaders.SET_COOKIE);
        if (received == null) return;
        try {
            cookies.put(uri, Map.of(HttpHeaders.SET_COOKIE, received));
        } catch (IOException | IllegalArgumentException ignored) {
            throw new MigrationException(
                    UPSTREAM_ERROR, "Apollo Portal returned an invalid session cookie");
        }
    }

    private HttpHeaders cookieHeaders(URI uri) {
        var headers = new HttpHeaders();
        try {
            cookies.get(uri, Map.of()).forEach(headers::addAll);
        } catch (IOException ignored) {
            throw new MigrationException(
                    UPSTREAM_ERROR, "Apollo Portal session cookie is unavailable");
        }
        return headers;
    }

    private static boolean isLoginPage(HttpTransport.Response response) {
        return response.body().stripLeading().startsWith("<")
                && loginFields(response.body()).containsKey("password");
    }

    private static Map<String, String> loginFields(String html) {
        Map<String, String> fields = new HashMap<>();
        var inputs = INPUT.matcher(html);
        while (inputs.find()) {
            String name = null;
            String value = "";
            var attrs = ATTRIBUTE.matcher(inputs.group());
            while (attrs.find()) {
                String attribute = attrs.group(1);
                String content = attrs.group(2);
                if (content == null) content = attrs.group(3);
                if (content == null) content = attrs.group(4);
                if (attribute.equalsIgnoreCase("name")) name = HtmlUtils.htmlUnescape(content);
                if (attribute.equalsIgnoreCase("value")) value = HtmlUtils.htmlUnescape(content);
            }
            if (name != null) fields.put(name, value);
        }
        return fields;
    }

    private static MigrationException authenticationFailed() {
        return new MigrationException(AUTHENTICATION_FAILED, "Apollo Portal denied the login session");
    }
}
