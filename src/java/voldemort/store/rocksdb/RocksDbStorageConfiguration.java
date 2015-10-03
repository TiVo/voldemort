package voldemort.store.rocksdb;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.util.SizeUnit;

import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StorageInitializationException;
import voldemort.store.StoreBinaryFormat;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;

public class RocksDbStorageConfiguration implements StorageConfiguration {

    static {
        RocksDB.loadLibrary();
    }

    private final int lockStripes;

    public static final String TYPE_NAME = "rocksdb";

    private static Logger logger = Logger.getLogger(RocksDbStorageConfiguration.class);

    private final VoldemortConfig voldemortconfig;

    private Map<String, RocksDbStorageEngine> stores = new HashMap<String, RocksDbStorageEngine>();

    public RocksDbStorageConfiguration(VoldemortConfig config) {
        /**
         * - TODO 1. number of default locks need to debated. This default is
         * same as that of Krati's. 2. Later add the property to VoldemortConfig
         */
        this.voldemortconfig = config;
        this.lockStripes = config.getAllProps().getInt("rocksdb.lock.stripes", 50);
    }

    @Override
    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef,
                                                             RoutingStrategy strategy) {
        String storeName = storeDef.getName();

        if(!stores.containsKey(storeName)) {
            String dataDir = this.voldemortconfig.getRdbDataDirectory() + "/" + storeName;

            new File(dataDir).mkdirs();

            try {
                Options rdbOptions = new Options()
                        .setCreateIfMissing(true)
                        .createStatistics()
                        .setCompactionStyle(voldemortconfig.getRocksdbCompactionStyle())
                        .setCompressionType(voldemortconfig.getRocksdbCompressionType())
                        .setLevelZeroFileNumCompactionTrigger(voldemortconfig.getRocksdbLevelZeroFileNumCompactionTrigger())
                        .setLevelZeroSlowdownWritesTrigger(voldemortconfig.getRocksdbLevelZeroSlowdownWritesTrigger())
                        .setLevelZeroStopWritesTrigger(voldemortconfig.getRocksdbLevelZeroStopWritesTrigger())
                        .setMaxBackgroundCompactions(voldemortconfig.getRocksdbMaxBackgroundCompactions())
                        .setMaxBackgroundFlushes(voldemortconfig.getRocksdbMaxBackgroundFlushes())
                        .setMaxBytesForLevelBase(voldemortconfig.getRocksdbMaxBytesForLevelBase())
                        .setMaxWriteBufferNumber(voldemortconfig.getRocksdbMaxWriteBufferNumber())
                        .setStatsDumpPeriodSec(voldemortconfig.getRocksdbStatsDumpPeriodSec())
                        .setTargetFileSizeBase(voldemortconfig.getRocksdbTargetFileSizeBase())
                        .setTargetFileSizeMultiplier(voldemortconfig.getRocksdbTargetFileSizeMultiplier())
                        .setWriteBufferSize(voldemortconfig.getRocksdbWriteBufferSize());

                logDbOptions(rdbOptions);

                RocksDB rdbStore = null;
                RocksDbStorageEngine rdbStorageEngine;
                if(this.voldemortconfig.getRocksdbPrefixKeysWithPartitionId()) {
                    rdbOptions.useFixedLengthPrefixExtractor(StoreBinaryFormat.PARTITIONID_PREFIX_SIZE);
                    rdbStore = RocksDB.open(rdbOptions, dataDir);
                    rdbStorageEngine = new PartitionPrefixedRocksDbStorageEngine(storeName,
                                                                                 rdbStore,
                                                                                 lockStripes,
                                                                                 strategy,
                                                                                 voldemortconfig.isRocksdbEnableReadLocks());
                } else {
                    rdbStore = RocksDB.open(rdbOptions, dataDir);
                    rdbStorageEngine = new RocksDbStorageEngine(storeName,
                                                                rdbStore,
                                                                lockStripes,
                                                                voldemortconfig.isRocksdbEnableReadLocks());
                }
                stores.put(storeName, rdbStorageEngine);
            } catch(Exception e) {
                throw new StorageInitializationException(e);
            }
        }

        return stores.get(storeName);
    }

    private void logDbOptions(Options rdbOptions) {
        Options defaults = new Options();
        logger.info("RocksBD Option: compactionStyle = " + rdbOptions.compactionStyle() + " (default = " + defaults.compactionStyle() + ")");
        logger.info("RocksBD Option: compressionType = " + rdbOptions.compressionType() + " (default = " + defaults.compressionType() + ")");
        logger.info("RocksBD Option: levelZeroFileNumCompactionTrigger = " + rdbOptions.levelZeroFileNumCompactionTrigger() + " (default = " + defaults.levelZeroFileNumCompactionTrigger() + ")");
        logger.info("RocksBD Option: levelZeroSlowdownWritesTrigger = " + rdbOptions.levelZeroSlowdownWritesTrigger() + " (default = " + defaults.levelZeroSlowdownWritesTrigger() + ")");
        logger.info("RocksBD Option: levelZeroStopWritesTrigger = " + rdbOptions.levelZeroStopWritesTrigger() + " (default = " + defaults.levelZeroStopWritesTrigger() + ")");
        logger.info("RocksBD Option: maxBackgroundCompactions = " + rdbOptions.maxBackgroundCompactions() + " (default = " + defaults.maxBackgroundCompactions() + ")");
        logger.info("RocksBD Option: maxBackgroundFlushes = " + rdbOptions.maxBackgroundFlushes() + " (default = " + defaults.maxBackgroundFlushes() + ")");
        logger.info("RocksBD Option: maxBytesForLevelBase = " + rdbOptions.maxBytesForLevelBase() + " (default = " + defaults.maxBytesForLevelBase() + ")");
        logger.info("RocksBD Option: maxWriteBufferNumber = " + rdbOptions.maxWriteBufferNumber() + " (default = " + defaults.maxWriteBufferNumber() + ")");
        logger.info("RocksBD Option: statsDumpPeriodSec = " + rdbOptions.statsDumpPeriodSec() + " (default = " + defaults.statsDumpPeriodSec() + ")");
        logger.info("RocksBD Option: targetFileSizeBase = " + rdbOptions.targetFileSizeBase() + " (default = " + defaults.targetFileSizeBase() + ")");
        logger.info("RocksBD Option: targetFileSizeMultiplier = " + rdbOptions.targetFileSizeMultiplier() + " (default = " + defaults.targetFileSizeMultiplier() + ")");
        logger.info("RocksBD Option: writeBufferSize = " + rdbOptions.writeBufferSize() + " (default = " + defaults.writeBufferSize() + ")");
    }

    @Override
    public String getType() {
        return TYPE_NAME;
    }

    @Override
    public void update(StoreDefinition storeDef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        for(RocksDbStorageEngine rdbStorageEngine: stores.values()) {
            rdbStorageEngine.getRocksDB().close();
        }

        stores.clear();
    }

    @Override
    public void removeStorageEngine(StorageEngine<ByteArray, byte[], byte[]> engine) {
        RocksDbStorageEngine rdbStorageEngine = (RocksDbStorageEngine) engine;

        rdbStorageEngine.getRocksDB().close();

        stores.remove(rdbStorageEngine.getName());
    }
}
