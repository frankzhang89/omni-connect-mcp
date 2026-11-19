package xyz.fz.omni.config.adapter;

import static xyz.fz.omni.config.core.MigrationException.Code.*;

import org.springframework.http.HttpHeaders;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.util.HtmlUtils;

import xyz.fz.omni.config.core.MigrationException;

import java.io.IOException;
import java.net.CookieManager;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/** One app-list call's legacy Portal login session. */
final class ApolloPortalSession {
    private static final Pattern INPUT =
            Pattern.compile(
                    "<input\\b(?:[^>\"']++|\"[^\"]*\"|'[^']*')*+>", Pattern.CASE_INSENSITIVE);
    private static final Pattern ATTRIBUTE =
            Pattern.compile("\\s+([\\w:-]+)\\s*=\\s*(?:\"([^\"]*)\"|'([^']*)'|([^\\s>]+))");

    private final HttpTransport http;
    private final URI signIn;
    private final URI apps;
    private final CookieManager cookies = new CookieManager();

    ApolloPortalSession(HttpTransport http, String portal) {
        this.http = http;
        signIn = URI.create(portal + "/signin");
        apps = URI.create(portal + "/apps");
    }

    HttpTransport.Response listApps(String username, String password) {
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
            if (signIn.getPath().equals(redirect(login, signIn).getPath())) {
                throw authenticationFailed();
            }
        } else {
            http.requireSuccess(login);
            if (isLoginPage(login)) throw authenticationFailed();
        }
        rememberCookies(signIn, login);

        var response = http.get(apps, cookieHeaders(apps));
        if ((response.status() == 302 || response.status() == 303)
                && signIn.getPath().equals(redirect(response, apps).getPath())) {
            throw authenticationFailed();
        }
        http.requireSuccess(response);
        if (isLoginPage(response)) throw authenticationFailed();
        return response;
    }

    private URI redirect(HttpTransport.Response response, URI request) {
        String location = response.headers().getFirst(HttpHeaders.LOCATION);
        if (location != null) {
            try {
                URI target = request.resolve(location);
                if (signIn.getScheme().equalsIgnoreCase(target.getScheme())
                        && signIn.getRawAuthority().equalsIgnoreCase(target.getRawAuthority())) {
                    return target;
                }
            } catch (IllegalArgumentException ignored) {
                // Do not expose a malformed redirect or follow it.
            }
        }
        throw new MigrationException(
                UPSTREAM_ERROR, "Apollo Portal returned an unsupported redirect");
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
