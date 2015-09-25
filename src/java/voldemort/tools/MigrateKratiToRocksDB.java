package voldemort.tools;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;
import voldemort.client.RoutingTier;
import voldemort.common.service.SchedulerService;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.storage.StorageService;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.krati.KratiStorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.rocksdb.RocksDbStorageConfiguration;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.SystemTime;
import voldemort.versioning.Versioned;

/**
 * Migrate all the Krati stores to RocksDB stores.  This class only supports the RocksDbStorageEngine (and not the
 * PartitionPrefixedRocksDbStorageEngine).
 */
public class MigrateKratiToRocksDB {

    private static Logger logger = Logger.getLogger(MigrateKratiToRocksDB.class);
    private static final long LOG_INTERVAL_MS = 5 * 60 * 1000;

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("USAGE: java voldemort.tools.MigrateDataCLI <voldemort_home>");
            System.exit(1);
        }

        String voldemortHome = args[0];
        VoldemortConfig config = VoldemortConfig.loadFromVoldemortHome(voldemortHome);
        config.setEnableSlop(false);
        MetadataStore metadata = MetadataStore.readFromDirectory(new File(config.getMetadataDirectory()), config.getNodeId());

        StoreRepository storeRepository = new StoreRepository();
        SchedulerService scheduler = new SchedulerService(config.getSchedulerThreads(), SystemTime.INSTANCE);
        StorageService storageService = new StorageService(storeRepository, metadata, scheduler, config);

        logger.info("Starting Voldemort Storage Service");
        storageService.start();

        RocksDbStorageConfiguration rocksdbConfig = new RocksDbStorageConfiguration(config);

        List<StorageEngine<ByteArray, byte[], byte[]>> kratiStores = storeRepository.getAllStorageEngines();
        for (StorageEngine<ByteArray, byte[], byte[]> kratiStore : kratiStores) {
            if (kratiStore instanceof KratiStorageEngine) {

                logger.info("Create RocksDB store for: " + kratiStore.getName());
                // The only value actually used is the name.  The others just can't be null.
                StoreDefinition rocksdbDefinition = new StoreDefinitionBuilder()
                        .setName(kratiStore.getName())
                        .setType(RocksDbStorageConfiguration.TYPE_NAME)
                        .setKeySerializer(new SerializerDefinition("null"))
                        .setValueSerializer(new SerializerDefinition("null"))
                        .setRoutingPolicy(RoutingTier.CLIENT)
                        .setReplicationFactor(1)
                        .setRequiredReads(1)
                        .setRequiredWrites(1)
                        .build();
                // The RoutingStrategy would be required to use the PartitionPrefixedRocksDbStorageEngine.  This
                // code assume we are using the RocksDbStorageEngine (rocksdb.prefix.keys.with.partitionid=false)
                StorageEngine<ByteArray, byte[], byte[]> rocksdbStore = rocksdbConfig.getStore(rocksdbDefinition, null);

                logger.info("Starting migration of: " + kratiStore.getName());
                long numEntries = 0;
                long lastLogTime = System.currentTimeMillis();
                ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> kratiEntries = kratiStore.entries();
                while (kratiEntries.hasNext()) {
                    Pair<ByteArray, Versioned<byte[]>> kratiEntry = kratiEntries.next();
                    rocksdbStore.put(kratiEntry.getFirst(), kratiEntry.getSecond(), null);
                    numEntries++;
                    if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MS) {
                        logger.info("Continuing migration of: " + kratiStore.getName() + "; Number of entries migrated: " + numEntries);
                        lastLogTime = System.currentTimeMillis();
                    }
                }
                kratiEntries.close();
                rocksdbStore.close();
                logger.info("Completed migration of: " + kratiStore.getName() + "; Number of entries migrated: " + numEntries);
            }
        }

        logger.info("Stopping Voldemort Storage Service");
        storageService.stop();
    }
}
