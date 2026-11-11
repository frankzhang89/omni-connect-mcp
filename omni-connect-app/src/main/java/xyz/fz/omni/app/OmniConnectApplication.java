package xyz.fz.omni.app;

import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.server.McpSyncServer;

import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import xyz.fz.omni.config.ConfigConnectorModule;
import xyz.fz.omni.core.*;
import xyz.fz.omni.core.launcher.LauncherLifecycleListener;

import java.util.*;

/** Single entry point for independently owned HTTP and stdio processes. */
@SpringBootApplication(
        scanBasePackages = "xyz.fz.omni",
        exclude = DataSourceAutoConfiguration.class,
        excludeName = {
            "org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration",
            "org.springframework.boot.data.redis.autoconfigure.DataRedisRepositoriesAutoConfiguration"
        })
public class OmniConnectApplication {
    public static void main(String[] args) {
        var app = new SpringApplication(OmniConnectApplication.class);
        app.setDefaultProperties(
                Map.of("spring.ai.mcp.server.annotation-scanner.enabled", "false"));
        app.setBannerMode(Banner.Mode.OFF);
        app.addListeners(new LauncherLifecycleListener());
        app.run(args);
    }

    @Bean
    ConnectorRegistry connectorRegistry() {
        return new ConnectorRegistry();
    }

    @Bean
    List<SyncToolSpecification> baseTools(
            ConnectorRegistry registry,
            List<ConnectorModule> modules,
            ConfigConnectorModule configs) {
        var tools = new ArrayList<>(ConnectorRegistry.specifications(registry));
        for (var module : modules)
            tools.addAll(ConnectorRegistry.specifications(module.managementTools()));
        tools.addAll(configs.conversionTools());
        return tools;
    }

    @Bean
    Startup startup(
            ConnectorRegistry registry, List<ConnectorModule> modules, McpSyncServer server) {
        return new Startup(registry, modules, server);
    }

    record Startup(
            ConnectorRegistry registry, List<ConnectorModule> modules, McpSyncServer server) {
        @EventListener(ApplicationReadyEvent.class)
        @Order(Ordered.HIGHEST_PRECEDENCE)
        public void ready() {
            registry.attach(server, modules.stream().map(ConnectorModule::id).toList());
            modules.forEach(ConnectorModule::start);
        }
    }
}
