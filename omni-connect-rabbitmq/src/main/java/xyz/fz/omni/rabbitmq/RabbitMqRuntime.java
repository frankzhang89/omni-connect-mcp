package xyz.fz.omni.rabbitmq;

import com.rabbitmq.client.*;

import tools.jackson.databind.json.JsonMapper;

import xyz.fz.omni.core.MessageSupport;
import xyz.fz.omni.core.MessageSupport.Failure;

import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RabbitMqRuntime implements AutoCloseable {
    private static final JsonMapper JSON = new JsonMapper();
    private final HttpClient http;
    private final String base, authorization;
    final String vhost;
    final Connection amqp;
    final int timeout, preview, maxPayload, maxResponse;

    RabbitMqRuntime(
            String base,
            String user,
            String password,
            String vhost,
            Connection amqp,
            int timeout,
            int preview,
            int maxPayload,
            int maxResponse) {
        URI uri = URI.create(base);
        if (!"http".equals(uri.getScheme())
                || uri.getHost() == null
                || uri.getUserInfo() != null
                || uri.getQuery() != null
                || uri.getFragment() != null)
            throw new IllegalArgumentException("Expected HTTP management URL without credentials");
        this.base = base.replaceAll("/+$", "");
        this.http =
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .followRedirects(HttpClient.Redirect.NEVER)
                        .build();
        this.vhost = vhost;
        this.amqp = amqp;
        this.timeout = timeout;
        this.preview = preview;
        this.maxPayload = maxPayload;
        this.maxResponse = maxResponse;
        authorization =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (user + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }

    String path(String kind) {
        return "/api/" + kind + "/" + enc(vhost);
    }

    Object request(String method, String path, Object body) throws Exception {
        var builder =
                HttpRequest.newBuilder(URI.create(base + path))
                        .timeout(Duration.ofMillis(MessageSupport.remainingMs(timeout)))
                        .header("Authorization", authorization)
                        .header("Content-Type", "application/json");
        builder.method(
                method,
                body == null
                        ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body)));
        var response = http.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
        byte[] bytes;
        try (var stream = response.body();
                var worker = Executors.newVirtualThreadPerTaskExecutor()) {
            var future = worker.submit(() -> stream.readNBytes(2 * 1024 * 1024 + 1));
            try {
                bytes = future.get(MessageSupport.remainingMs(timeout), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                stream.close();
                future.cancel(true);
                throw e;
            }
        }
        int status = response.statusCode();
        if (status == 401)
            throw new Failure("AUTHENTICATION_FAILED", "Management authentication failed");
        if (status == 403) throw new Failure("PERMISSION_DENIED", "Management permission denied");
        if (status == 404) throw new Failure("RESOURCE_NOT_FOUND", "Management resource not found");
        if (status < 200 || status >= 300)
            throw new Failure("BROKER_UNAVAILABLE", "Management request failed");
        if (bytes.length > 2 * 1024 * 1024)
            throw new Failure("RESPONSE_TOO_LARGE", "Narrow the query");
        return JSON.readValue(bytes, Object.class);
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> object(Object value) {
        return (Map<String, Object>) value;
    }

    Map<String, Object> page(Object response, String filter, int page, int size) {
        if (response instanceof Map<?, ?>) return object(response);
        List<?> all = (List<?>) response;
        var matches =
                all.stream()
                        .filter(
                                o ->
                                        filter == null
                                                || String.valueOf(((Map<?, ?>) o).get("name"))
                                                        .contains(filter))
                        .toList();
        int from = (int) Math.min((long) (page - 1) * size, matches.size());
        return Map.of(
                "items",
                matches.subList(from, Math.min(from + size, matches.size())),
                "page",
                page,
                "pageSize",
                size,
                "total",
                matches.size());
    }

    Map<String, Object> queues(String name, int page, int size) throws Exception {
        String query =
                "?page="
                        + page
                        + "&page_size="
                        + size
                        + "&pagination=true&columns=name,type,messages,messages_ready,messages_unacknowledged,consumers";
        if (name != null) query += "&name=" + enc(name) + "&use_regex=false";
        return page(request("GET", path("queues") + query, null), name, page, size);
    }

    Map<String, Object> inspectQueue(String queue) throws Exception {
        var raw =
                object(
                        request(
                                "GET",
                                path("queues") + "/" + enc(MessageSupport.required(queue)),
                                null));
        var out = new LinkedHashMap<String, Object>();
        for (String k :
                List.of(
                        "name",
                        "vhost",
                        "type",
                        "state",
                        "messages",
                        "messages_ready",
                        "messages_unacknowledged",
                        "consumers",
                        "message_stats",
                        "arguments",
                        "policy",
                        "operator_policy",
                        "effective_policy_definition")) out.put(k, raw.get(k));
        return out;
    }

    Map<String, Object> bindings(String queue, String exchange, int page, int size)
            throws Exception {
        if ((queue == null) == (exchange == null)) throw new IllegalArgumentException();
        Object response;
        if (queue != null)
            response =
                    request(
                            "GET",
                            path("queues")
                                    + "/"
                                    + enc(MessageSupport.required(queue))
                                    + "/bindings",
                            null);
        else {
            var all = new ArrayList<Object>();
            for (String direction : List.of("source", "destination"))
                all.addAll(
                        (List<?>)
                                request(
                                        "GET",
                                        path("exchanges")
                                                + "/"
                                                + enc(exchange)
                                                + "/bindings/"
                                                + direction,
                                        null));
            response = all;
        }
        return page(response, null, page, size);
    }

    Map<String, Object> consumers(String queue, int page, int size) throws Exception {
        Object response;
        if (queue == null) response = request("GET", path("consumers"), null);
        else
            response =
                    object(
                                    request(
                                            "GET",
                                            path("queues")
                                                    + "/"
                                                    + enc(MessageSupport.required(queue))
                                                    + "?columns=consumer_details",
                                            null))
                            .getOrDefault("consumer_details", List.of());
        return page(response, null, page, size);
    }

    Map<String, Object> channel(String name) throws Exception {
        var raw =
                object(request("GET", "/api/channels/" + enc(MessageSupport.required(name)), null));
        var out = new LinkedHashMap<String, Object>();
        for (String k :
                List.of(
                        "name",
                        "number",
                        "vhost",
                        "state",
                        "messages_unacknowledged",
                        "prefetch_count",
                        "global_prefetch_count",
                        "consumer_count",
                        "connection_details",
                        "message_stats")) out.put(k, raw.get(k));
        if (raw.get("connection_details") instanceof Map<?, ?> c && c.get("name") != null) {
            var connection =
                    object(
                            request(
                                    "GET",
                                    "/api/connections/" + enc(c.get("name").toString()),
                                    null));
            var summary = new LinkedHashMap<String, Object>();
            for (String k : List.of("name", "peer_host", "peer_port", "state"))
                summary.put(k, connection.get(k));
            if (connection.get("client_properties") instanceof Map<?, ?> props)
                summary.put("clientProvidedName", props.get("connection_name"));
            out.put("connection", summary);
        }
        return out;
    }

    Map<String, Object> sample(String queue, int count) throws Exception {
        Object result =
                request(
                        "POST",
                        path("queues") + "/" + enc(MessageSupport.required(queue)) + "/get",
                        Map.of(
                                "count",
                                count,
                                "ackmode",
                                "ack_requeue_true",
                                "encoding",
                                "base64",
                                "truncate",
                                preview));
        var messages = new ArrayList<Map<String, Object>>();
        for (Object value : (List<?>) result) {
            var item = new LinkedHashMap<String, Object>(object(value));
            if (item.get("payload") instanceof String payload) {
                byte[] bytes = Base64.getDecoder().decode(payload);
                var shown = MessageSupport.preview(bytes, preview);
                item.putAll(shown);
                long original =
                        item.get("payload_bytes") instanceof Number n
                                ? n.longValue()
                                : bytes.length;
                item.put("sizeBytes", original);
                item.put("truncated", original > bytes.length || original > preview);
                item.remove("payload_encoding");
                item.remove("payload_bytes");
            }
            if (MessageSupport.jsonSize(messages) + MessageSupport.jsonSize(item) > maxResponse)
                throw new Failure(
                        "RESPONSE_TOO_LARGE",
                        "Sample requeued; reduce count",
                        Map.of("messages", messages, "requeued", true));
            messages.add(item);
        }
        return Map.of("messages", messages, "requeued", true, "sideEffects", true);
    }

    private Channel open() throws Exception {
        if (amqp == null)
            throw new Failure(
                    "CAPABILITY_NOT_CONFIGURED", "Configure AMQP addresses using rabbitmq_connect");
        return amqp.createChannel();
    }

    private Object presentHeader(Object value, int depth) {
        if (value == null || value instanceof Number || value instanceof Boolean) return value;
        if (depth > 4) return Map.of("truncated", true);
        if (value instanceof Map<?, ?> map) {
            var result = new LinkedHashMap<String, Object>();
            for (var entry : map.entrySet().stream().limit(64).toList())
                result.put(
                        String.valueOf(entry.getKey()), presentHeader(entry.getValue(), depth + 1));
            return result;
        }
        if (value instanceof List<?> list)
            return list.stream().limit(64).map(v -> presentHeader(v, depth + 1)).toList();
        byte[] bytes =
                value instanceof LongString text
                        ? text.getBytes()
                        : value instanceof byte[] binary
                                ? binary
                                : String.valueOf(value).getBytes(StandardCharsets.UTF_8);
        var result = MessageSupport.preview(bytes, preview);
        return result.get("encoding").equals("utf8")
                        && Boolean.FALSE.equals(result.get("truncated"))
                ? result.get("payload")
                : result;
    }

    Map<String, Object> publish(
            String exchange,
            String routingKey,
            String payload,
            String encoding,
            Map<String, String> headers,
            String contentType,
            String messageId,
            String correlationId,
            Boolean persistent)
            throws Exception {
        if (exchange == null || routingKey == null) throw new IllegalArgumentException();
        byte[] bytes = MessageSupport.payload(payload, encoding, maxPayload);
        MessageSupport.headers(headers);
        Channel c = open();
        try {
            c.confirmSelect();
            var returned = new AtomicBoolean();
            c.addReturnListener(r -> returned.set(true));
            var properties =
                    new AMQP.BasicProperties.Builder()
                            .headers(headers == null ? Map.of() : new HashMap<>(headers))
                            .contentType(contentType)
                            .messageId(messageId)
                            .correlationId(correlationId)
                            .deliveryMode(Boolean.FALSE.equals(persistent) ? 1 : 2)
                            .build();
            try {
                c.basicPublish(exchange, routingKey, true, properties, bytes);
                boolean confirmed = c.waitForConfirms(MessageSupport.remainingMs(timeout));
                if (returned.get())
                    throw new Failure("UNROUTABLE", "Message was returned as unroutable");
                if (!confirmed) throw new Failure("PUBLISH_REJECTED", "Broker rejected publish");
                return Map.of("outcome", "CONFIRMED", "sizeBytes", bytes.length);
            } catch (Failure e) {
                throw e;
            } catch (Exception e) {
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                Failure known = protocolFailure(e);
                if (known != null) throw known;
                throw new Failure(
                        "PUBLISH_RESULT_UNKNOWN", "Publish outcome unknown; do not blindly retry");
            }
        } finally {
            closeChannel(c);
        }
    }

    private static void closeChannel(Channel channel) {
        try {
            channel.close();
        } catch (Exception ignored) {
            /* Never hide known progress. */
        }
    }

    private static Failure protocolFailure(Throwable error) {
        for (Throwable e = error; e != null; e = e.getCause()) {
            if (e instanceof ShutdownSignalException signal
                    && signal.getReason() instanceof AMQP.Channel.Close reason) {
                if (reason.getReplyCode() == 404)
                    return new Failure("RESOURCE_NOT_FOUND", "AMQP resource not found");
                if (reason.getReplyCode() == 403)
                    return new Failure("PERMISSION_DENIED", "AMQP permission denied");
                if (reason.getReplyCode() == 406)
                    return new Failure("INVALID_ARGUMENT", "AMQP precondition failed");
            }
        }
        return null;
    }

    Map<String, Object> consume(String queue, int count, int wait) throws Exception {
        MessageSupport.required(queue);
        var messages = new ArrayList<Map<String, Object>>();
        int ackSent = 0;
        Channel c = open();
        try {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(wait);
            try {
                while (messages.size() < count && System.nanoTime() < deadline) {
                    GetResponse response = c.basicGet(queue, false);
                    if (response == null) {
                        Thread.sleep(Math.min(100, wait));
                        continue;
                    }
                    var item =
                            new LinkedHashMap<String, Object>(
                                    MessageSupport.preview(response.getBody(), preview));
                    var p = response.getProps();
                    item.put("messageId", p.getMessageId());
                    item.put("correlationId", p.getCorrelationId());
                    item.put("headers", presentHeader(p.getHeaders(), 0));
                    item.put(
                            "headersTruncated",
                            p.getHeaders() != null && p.getHeaders().size() > 64);
                    item.put("contentType", p.getContentType());
                    item.put("exchange", response.getEnvelope().getExchange());
                    item.put("routingKey", response.getEnvelope().getRoutingKey());
                    item.put("redelivered", response.getEnvelope().isRedeliver());
                    if (MessageSupport.jsonSize(messages) + MessageSupport.jsonSize(item)
                            > maxResponse) {
                        c.basicNack(response.getEnvelope().getDeliveryTag(), false, true);
                        break;
                    }
                    messages.add(item);
                    c.basicAck(response.getEnvelope().getDeliveryTag(), false);
                    ackSent++;
                }
                return Map.of("messages", messages, "ackStatus", "SENT", "ackSent", ackSent);
            } catch (Exception e) {
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                if (messages.isEmpty()) {
                    Failure known = protocolFailure(e);
                    if (known != null) throw known;
                }
                throw new Failure(
                        "CONSUME_RESULT_UNKNOWN",
                        "Consumption interrupted; inspect progress before retry",
                        Map.of("messages", messages, "ackSent", ackSent));
            }
        } finally {
            closeChannel(c);
        }
    }

    public void close() {
        http.close();
        if (amqp != null)
            try {
                amqp.close(200, "closing", 1000);
            } catch (Exception ignored) {
            }
    }
}
