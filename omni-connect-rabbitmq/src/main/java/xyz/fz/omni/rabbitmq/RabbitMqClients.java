package xyz.fz.omni.rabbitmq;

import com.rabbitmq.client.*;

import org.springframework.stereotype.Component;

/** Injectable boundary; no connections are opened until activation. */
@Component
public class RabbitMqClients {
    public Connection connect(
            String addresses, String username, String password, String vhost, int timeout)
            throws Exception {
        var f = new ConnectionFactory();
        f.setUsername(username);
        f.setPassword(password);
        f.setVirtualHost(vhost);
        f.setConnectionTimeout(timeout);
        f.setHandshakeTimeout(timeout);
        f.setChannelRpcTimeout(timeout);
        f.setAutomaticRecoveryEnabled(false);
        return f.newConnection(Address.parseAddresses(addresses), "omni-connect-mcp");
    }
}
