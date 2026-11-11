package xyz.fz.omni.config.core;

import xyz.fz.omni.config.core.ConfigModels.*;

import java.util.Set;

public interface ConfigStore {
    /** Identifies the configuration center implemented by this adapter. */
    Provider provider();

    /** Declares implemented operations without advertising reserved future capabilities. */
    Set<Capability> capabilities();

    interface Readable extends ConfigStore {
        /** Reads a complete document with its location, declared format, and content hash. */
        ReadResult read(ReadRequest request);
    }

    interface Writable extends ConfigStore {
        /** Creates or replaces a complete document and reports the upstream acknowledgement. */
        PublishResult write(WriteRequest request);
    }

    interface ItemReadable extends ConfigStore {
        ItemReadResult readItem(ItemReadRequest request);
    }

    interface ItemUpdatable extends ConfigStore {
        ItemUpdateResult updateItem(ItemUpdateRequest request);
    }

    interface Discoverable extends ConfigStore {
        NamespaceListResult listNamespaces();

        DataIdCountResult countDataIds(String namespace, String group);
    }
}
