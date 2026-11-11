package xyz.fz.omni.app;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/** A local RESP stand-in covering Lettuce handshake and the string operations used here. */
final class RedisFixture implements AutoCloseable {
    private final ServerSocket listener;
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
    private final Map<Integer, Map<String, String>> values = new ConcurrentHashMap<>();
    private volatile boolean closed;

    RedisFixture() throws IOException {
        listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread.ofVirtual()
                .start(
                        () -> {
                            while (!closed) {
                                try {
                                    Socket client = listener.accept();
                                    clients.add(client);
                                    Thread.ofVirtual().start(() -> serve(client));
                                } catch (IOException e) {
                                    if (!closed) throw new UncheckedIOException(e);
                                }
                            }
                        });
    }

    int port() {
        return listener.getLocalPort();
    }

    private static String line(InputStream in) throws IOException {
        var data = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                if (in.read() != '\n') throw new IOException("Invalid RESP");
                return data.toString(StandardCharsets.UTF_8);
            }
            data.write(b);
        }
        throw new EOFException();
    }

    private void serve(Socket socket) {
        try (socket) {
            var in = socket.getInputStream();
            var out = socket.getOutputStream();
            int db = 0;
            while (!closed) {
                int count = Integer.parseInt(line(in).substring(1));
                var args = new ArrayList<String>();
                for (int i = 0; i < count; i++) {
                    int size = Integer.parseInt(line(in).substring(1));
                    args.add(new String(in.readNBytes(size), StandardCharsets.UTF_8));
                    line(in);
                }
                var data = values.computeIfAbsent(db, k -> new ConcurrentHashMap<>());
                String result;
                switch (args.getFirst().toUpperCase(Locale.ROOT)) {
                    case "HELLO" -> result = "-ERR unknown command HELLO\r\n";
                    case "PING" -> result = "+PONG\r\n";
                    case "SELECT" -> {
                        db = Integer.parseInt(args.get(1));
                        result = "+OK\r\n";
                    }
                    case "CLIENT", "AUTH" -> result = "+OK\r\n";
                    case "SET" -> {
                        data.put(args.get(1), args.get(2));
                        result = "+OK\r\n";
                    }
                    case "GETRANGE", "GET" -> {
                        var value = data.get(args.get(1));
                        result =
                                value == null
                                        ? "$-1\r\n"
                                        : "$"
                                                + value.getBytes(StandardCharsets.UTF_8).length
                                                + "\r\n"
                                                + value
                                                + "\r\n";
                    }
                    case "TYPE" ->
                            result = data.containsKey(args.get(1)) ? "+string\r\n" : "+none\r\n";
                    case "TTL", "PTTL" ->
                            result = data.containsKey(args.get(1)) ? ":-1\r\n" : ":-2\r\n";
                    case "STRLEN" ->
                            result = ":" + data.getOrDefault(args.get(1), "").length() + "\r\n";
                    default -> result = "-ERR unsupported fixture command\r\n";
                }
                out.write(result.getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
        } catch (IOException ignored) {
            /* Disconnection is expected during generation cleanup. */
        } finally {
            clients.remove(socket);
        }
    }

    public void close() throws IOException {
        closed = true;
        listener.close();
        for (var client : clients) client.close();
    }
}
