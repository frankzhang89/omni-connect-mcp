package xyz.fz.omni.core;


/** A module supplies lightweight management endpoints and optionally activates a YAML target. */
public interface ConnectorModule {
    String id();

    Object managementTools();

    default void start() {}
}
