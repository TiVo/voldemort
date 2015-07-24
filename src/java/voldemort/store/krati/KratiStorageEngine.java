package voldemort.store.krati;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import krati.core.segment.SegmentFactory;
import krati.store.DynamicDataStore;
import krati.util.FnvHashFunction;

import krati.util.IndexedIterator;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.StripedLock;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class KratiStorageEngine extends AbstractStorageEngine<ByteArray, byte[], byte[]> {

    private static final Logger logger = Logger.getLogger(KratiStorageEngine.class);
    private final DynamicDataStore datastore;
    private final StripedLock locks;
    private final boolean persistOnWrite;

    public KratiStorageEngine(String name,
                              SegmentFactory segmentFactory,
                              int segmentFileSizeMB,
                              int lockStripes,
                              double hashLoadFactor,
                              int initLevel,
                              int batchSize,
                              int numSyncBatches,
                              File dataDirectory,
                              boolean persistOnWrite) {
        super(name);
        try {
            this.datastore = new DynamicDataStore(dataDirectory,
                                                  initLevel,
                                                  batchSize,
                                                  numSyncBatches,
                                                  segmentFileSizeMB,
                                                  segmentFactory,
                                                  hashLoadFactor,
                                                  new FnvHashFunction());
            this.locks = new StripedLock(lockStripes);
            this.persistOnWrite = persistOnWrite;
        } catch(Exception e) {
            throw new VoldemortException("Failure initializing store.", e);
        }

    }

    @Override
    public void close() throws VoldemortException {
        try {
            this.datastore.close();
        } catch(IOException e) {
            logger.error("Failed to close store '" + getName() + "': ", e);
            throw new VoldemortException(e);
        }
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys, null);
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        return StoreUtils.getVersions(get(key, null));
    }

    @Override
    public void truncate() {
        try {
            datastore.clear();
        } catch(Exception e) {
            logger.error("Failed to truncate store '" + getName() + "': ", e);
            throw new VoldemortException("Failed to truncate store '" + getName() + "'.");
        }
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        try {
            return disassembleValues(datastore.get(key.get()));
        } catch(Exception e) {
            logger.error("Error reading value: ", e);
            throw new VoldemortException("Error reading value: ", e);
        }
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        return new KratiClosableIterator(datastore.iterator());
    }

    @Override
    public ClosableIterator<ByteArray> keys() {
        return new KratiClosableKeyIterator(datastore.keyIterator());
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(int partition) {
        throw new UnsupportedOperationException("Partition based entries scan not supported for this storage type");
    }

    @Override
    public ClosableIterator<ByteArray> keys(int partition) {
        throw new UnsupportedOperationException("Partition based key scan not supported for this storage type");
    }

    @Override
    public boolean delete(ByteArray key, Version maxVersion) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        synchronized(this.locks.lockFor(key.get())) {
            if(maxVersion == null) {
                try {
                    boolean deleted = datastore.delete(key.get());
                    if (persistOnWrite) {
                        datastore.persist();
                    }
                    return deleted;
                } catch(Exception e) {
                    logger.error("Failed to delete key: ", e);
                    throw new VoldemortException("Failed to delete key: " + key, e);
                }
            }

            List<Versioned<byte[]>> returnedValuesList = this.get(key, null);

            // Case if there is nothing to delete
            if(returnedValuesList.size() == 0) {
                return false;
            }

            Iterator<Versioned<byte[]>> iter = returnedValuesList.iterator();
            while(iter.hasNext()) {
                Versioned<byte[]> currentValue = iter.next();
                Version currentVersion = currentValue.getVersion();
                if(currentVersion.compare(maxVersion) == Occurred.BEFORE) {
                    iter.remove();
                }
            }

            try {
                boolean deleted;
                if(returnedValuesList.size() == 0)
                    deleted = datastore.delete(key.get());
                else
                    deleted = datastore.put(key.get(), assembleValues(returnedValuesList));
                if(persistOnWrite) {
                    datastore.persist();
                }
                return deleted;
            } catch(Exception e) {
                String message = "Failed to delete key " + key;
                logger.error(message, e);
                throw new VoldemortException(message, e);
            }
        }
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);

        synchronized(this.locks.lockFor(key.get())) {
            // First get the value
            List<Versioned<byte[]>> existingValuesList = this.get(key, null);

            // If no value, add one
            if(existingValuesList.size() == 0) {
                existingValuesList = new ArrayList<Versioned<byte[]>>();
                existingValuesList.add(new Versioned<byte[]>(value.getValue(), value.getVersion()));
            } else {

                // Update the value
                List<Versioned<byte[]>> removedValueList = new ArrayList<Versioned<byte[]>>();
                for(Versioned<byte[]> versioned: existingValuesList) {
                    Occurred occurred = value.getVersion().compare(versioned.getVersion());
                    if(occurred == Occurred.BEFORE)
                        throw new ObsoleteVersionException("Obsolete version for key '" + key
                                                           + "': " + value.getVersion());
                    else if(occurred == Occurred.AFTER)
                        removedValueList.add(versioned);
                }
                existingValuesList.removeAll(removedValueList);
                existingValuesList.add(value);
            }

            try {
                datastore.put(key.get(), assembleValues(existingValuesList));
                if(persistOnWrite) {
                    datastore.persist();
                }
            } catch(Exception e) {
                String message = "Failed to put key " + key;
                logger.error(message, e);
                throw new VoldemortException(message, e);
            }
        }
    }

    @Override
    public List<Versioned<byte[]>> multiVersionPut(ByteArray key,
                                                   final List<Versioned<byte[]>> values)
            throws VoldemortException {
        // TODO the day this class implements getAndLock and putAndUnlock, this
        // method can be removed
        StoreUtils.assertValidKey(key);
        List<Versioned<byte[]>> valuesInStorage = null;
        List<Versioned<byte[]>> obsoleteVals = null;

        synchronized(this.locks.lockFor(key.get())) {
            valuesInStorage = this.get(key, null);
            obsoleteVals = resolveAndConstructVersionsToPersist(valuesInStorage, values);

            try {
                datastore.put(key.get(), assembleValues(valuesInStorage));
            } catch(Exception e) {
                String message = "Failed to put key " + key;
                logger.error(message, e);
                throw new VoldemortException(message, e);
            }
        }
        return obsoleteVals;
    }

    /**
     * Store the versioned values
     *
     * @param values list of versioned bytes
     * @return the list of versioned values rolled into an array of bytes
     */
    private byte[] assembleValues(List<Versioned<byte[]>> values) throws IOException {

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream dataStream = new DataOutputStream(stream);

        for(Versioned<byte[]> value: values) {
            byte[] object = value.getValue();
            dataStream.writeInt(object.length);
            dataStream.write(object);

            VectorClock clock = (VectorClock) value.getVersion();
            dataStream.writeInt(clock.sizeInBytes());
            dataStream.write(clock.toBytes());
        }

        return stream.toByteArray();
    }

    /**
     * Splits up value into multiple versioned values
     *
     * @param values
     * @return list versioned values
     * @throws IOException
     */
    private List<Versioned<byte[]>> disassembleValues(byte[] values) throws IOException {

        if(values == null)
            return new ArrayList<Versioned<byte[]>>(0);

        List<Versioned<byte[]>> returnList = new ArrayList<Versioned<byte[]>>();
        ByteArrayInputStream stream = new ByteArrayInputStream(values);
        DataInputStream dataStream = new DataInputStream(stream);

        while(dataStream.available() > 0) {
            byte[] object = new byte[dataStream.readInt()];
            dataStream.read(object);

            byte[] clockBytes = new byte[dataStream.readInt()];
            dataStream.read(clockBytes);
            VectorClock clock = new VectorClock(clockBytes);

            returnList.add(new Versioned<byte[]>(object, clock));
        }

        return returnList;
    }

    private class KratiClosableKeyIterator implements ClosableIterator<ByteArray> {
        
        private IndexedIterator<byte[]> iter;
        
        public KratiClosableKeyIterator(IndexedIterator<byte[]> iter) {
             this.iter = iter;
        }

        @Override
        public void close() {
            // Nothing to close here
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public ByteArray next() {
            return new ByteArray(iter.next());
        }

        @Override
        public void remove() {
            iter.remove();
        }
    }
    
    private class KratiClosableIterator implements
            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        private IndexedIterator<Map.Entry<byte[], byte[]>> iter;
        private ByteArray key = null;
        private List<Versioned<byte[]>> values = Collections.emptyList();
        private int index = 0;
        

        public KratiClosableIterator(IndexedIterator<Map.Entry<byte[], byte[]>> iter) {
            this.iter = iter;
        }

        @Override
        public void close() {
            // Nothing to close here
        }

        @Override
        public boolean hasNext() {
            return (index < values.size()) || iter.hasNext();
        }

        @Override
        public Pair<ByteArray, Versioned<byte[]>> next() {
            try {
                if (index >= values.size()) {
                    Map.Entry<byte[], byte[]> entry = iter.next();
                    key = new ByteArray(entry.getKey());
                    values = disassembleValues(entry.getValue());
                    index = 0;
                }
                return Pair.create(key, values.get(index++));
            } catch (IOException e) {
                String message = "Entries iteration failed";
                logger.error(message, e);
                throw new VoldemortException(message, e);
            }
        }

        @Override
        public void remove() {
            Pair<ByteArray, Versioned<byte[]>> currentPair = next();
            delete(currentPair.getFirst(), currentPair.getSecond().getVersion());
        }

    }
}
