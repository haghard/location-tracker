/*
 * Copyright (c) 2021-22 by Codelf Solutions. All rights reserved.
 */

package akka.cluster.ddata.durable.raf;

import akka.cluster.UniqueAddress;
import one.nio.async.AsyncExecutor;
import one.nio.lock.RWLock;
import one.nio.mem.SharedMemoryMap;
import one.nio.util.Hash;
import scala.Unit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/*

https://github.com/odnoklassniki/one-nio/blob/4acd7e344ef6058c1cdd0b83d74d866fc442c2b4/test/one/nio/mem/CustomSerializerMapTest.java


A generic solution for caching data in shared memory or memory-mapped files.
https://github.com/odnoklassniki/one-nio/wiki/Package-description
https://github.com/odnoklassniki/one-nio/blob/4acd7e344ef6058c1cdd0b83d74d866fc442c2b4/test/one/nio/mem/SharedMemoryFixedBlobMap.java


https://howtodoinjava.com/java7/nio/memory-mapped-files-mappedbytebuffer/
https://habr.com/en/company/odnoklassniki/blog/148139/
https://habr.com/en/company/odnoklassniki/blog/195004/

https://www.kdgregory.com/index.php?page=java.byteBuffer

Beyond ByteBuffers by Brian Goetz
https://youtu.be/iwSCtxMbBLI?list=PLbZ2T3O9BuvczX5j03bWMrMFzK5OAs9mZ


A range of tools for managing off-heap memory.

  DirectMemory: allows to allocate memory beyond Java Heap.
  MappedFile: maps and unmaps files to RAM. Supports files larger than 2 GB.
  Malloc: off-heap memory allocator that works in a given memory range.
  MallocMT: a special version of Malloc for multi-threaded applications.
  FixedSizeAllocator: extremely fast lock-free allocator that manages chunks of the fixed size.
  LongHashSet, LongLongHashMap, LongObjectHashMap: off-heap lock-free hash tables with 64-bit keys.
  OffheapMap: an abstraction for building general purpose off-heap hash tables.
  SharedMemory*Map: a generic solution for caching data in shared memory or memory-mapped files.

*/

//Off-heap persistent hash tables
public class SharedMemoryLongMap extends SharedMemoryMap<Long, SharedMemoryLongMap.SharedMemoryValue> {

    public SharedMemoryLongMap(int capacity, String fileName, long fileSize) throws IOException {
        super(capacity, fileName, fileSize);
    }

    @Override
    protected Long keyAt(long entry) {
        // Recover original key from a hashCode
        return Hash.twang_unmix(unsafe.getLong(entry + HASH_OFFSET));
    }

    @Override
    protected long hashCode(Long key) {
        // Shuffle bits in order to randomize buckets for close keys
        return Hash.twang_mix(key);
    }

    @Override
    protected boolean equalsAt(long entry, Long key) {
        // Equal hashCodes <=> equal keys
        return true;
    }

    public void forEach(Function<Record<Long, SharedMemoryValue>, Unit> f, int workers) {
        iterate((Visitor<Long, SharedMemoryValue>) rec -> f.apply(rec), workers);
    }

    // Called from CleanupThread. Returns true if the entry should be removed.
    // Can be used to perform custom logic on entry eviction.
    protected boolean shouldCleanup(long entry, long expirationTime) {
        return false;
    }

    public int valueSize(SharedMemoryValue value) {
        return sizeOf(value);
    }

    public static class SharedMemoryValue {
        public final Object envelope;//.asInstanceOf[DataEnvelope]
        public final byte[] digest;

        public SharedMemoryValue(Object env, byte[] digest) {
            this.envelope = env;
            this.digest = digest;
        }
    }

    public static class OffsetData {
        final int offset;
        final Map<String, byte[]> results;

        public OffsetData(int offset, Map<String, byte[]> data) {
            this.offset = offset;
            this.results = data;
        }

        public int getOffset() {
            return offset;
        }

        public Map<String, byte[]> getResults() {
            return results;
        }
    }

    public static class KeysEnvelope {
        final int offset;
        final scala.collection.mutable.Set<String> results;

        public KeysEnvelope(int offset, scala.collection.mutable.Set<String> data) {
            this.offset = offset;
            this.results = data;
        }

        public int getOffset() {
            return offset;
        }

        public scala.collection.mutable.Set<String> getResults() {
            return results;
        }
    }

    //TODO: Future -> CompletableFuture
    public CompletableFuture<KeysEnvelope> collectKeysAsync(int startIndex, int pageSize) {
        final CompletableFuture<KeysEnvelope> promise = new CompletableFuture<>();
        AsyncExecutor.submit(() -> {
            final KeysEnvelope env = collectKeys(startIndex, pageSize);
            promise.complete(env);
            return env;
        });
        return promise;
    }

    private KeysEnvelope collectKeys(int startIndex, int pageSize) {
        final scala.collection.mutable.Set<String> acc = new scala.collection.mutable.HashSet<>();
        int offset;
        int size = 0;
        for (offset = startIndex; offset < capacity && size < pageSize; offset += 1) { //search by segments
            long currentPtr = mapBase + (long) offset * 8;
            RWLock lock = locks[offset & (CONCURRENCY_LEVEL - 1)].lockRead();
            try {
                for (long entry; (entry = unsafe.getAddress(currentPtr)) != 0; currentPtr = entry + NEXT_OFFSET) { //search inside a segment
                    //acc.add(Long.toHexString(keyAt(entry)));
                    acc.add(keyAt(entry).toString());
                    size += 1;
                }
            } finally {
                lock.unlockRead();
            }
        }
        return new KeysEnvelope(offset, acc);
    }

    public CompletableFuture<OffsetData> collectRangeDigest(int index, int rangeSize) {
        final CompletableFuture<OffsetData> promise = new CompletableFuture<>();
        AsyncExecutor.submit(() -> {
            OffsetData data = collectDigests(index, rangeSize);
            promise.complete(data);
            return 1;  //data;
        });
        return promise;

        //AsyncExecutor.submit(() -> collectPageParallel(index, pageSize, f, promise));

        /*
        final CompletableFuture<Integer> promise = new CompletableFuture<>();
        AsyncExecutor.fork(1, (taskNum, taskCount) -> collectPageParallel(index, pageSize, f, promise));
        return promise;
        */
    }

    private OffsetData collectDigests(int index, int rangeSize) {
        int i;
        int j = 0;
        final Map<String, byte[]> map = new HashMap<>();
        for (i = index; i < capacity && j < rangeSize; i += 1) { //search by segments
            long currentPtr = mapBase + (long) i * 8;
            RWLock lock = locks[i & (CONCURRENCY_LEVEL - 1)].lockRead();
            try {
                for (long entry; (entry = unsafe.getAddress(currentPtr)) != 0; currentPtr = entry + NEXT_OFFSET) {  //search inside a segment
                    Long key = keyAt(entry);
                    SharedMemoryValue value = valueAt(entry);
                    //map.put(Long.toHexString(key), value.digest);
                    map.put(key.toString(), value.digest);
                    j += 1;
                }
            } finally {
                lock.unlockRead();
            }
        }
        return new OffsetData(i, map);
    }

    public <Env> CompletableFuture<scala.collection.Set<UniqueAddress>> collectRemovedNodesAsync(
            BiFunction<scala.collection.Set<UniqueAddress>, Env, scala.collection.Set<UniqueAddress>> agg
    ) {
        final CompletableFuture<scala.collection.Set<UniqueAddress>> promise = new CompletableFuture<>();
        AsyncExecutor.submit(() -> {
            promise.complete(collectNewAddress(agg));
            return 1;
        });
        return promise;
    }

    private <Env> scala.collection.Set<UniqueAddress> collectNewAddress(
            BiFunction<scala.collection.Set<UniqueAddress>, Env, scala.collection.Set<UniqueAddress>> agg
    ) {
        scala.collection.Set<UniqueAddress> acc = new scala.collection.immutable.HashSet<>();
        for (int index = 0; index < capacity; index += 1) { //search by segments
            long currentPtr = mapBase + (long) index * 8;
            RWLock lock = locks[index & (CONCURRENCY_LEVEL - 1)].lockRead();
            try {
                for (long entry; (entry = unsafe.getAddress(currentPtr)) != 0; currentPtr = entry + NEXT_OFFSET) { //search inside a segment
                    SharedMemoryValue value = valueAt(entry);
                    //keyAt(entry);
                    //sizeOf(entry);
                    Env envelope = (Env) value.envelope;
                    scala.collection.Set<UniqueAddress> updated = agg.apply(acc, envelope);
                    acc = acc.union(updated);
                }
            } finally {
                lock.unlockRead();
            }
        }
        return acc;
    }


    public <Env> CompletableFuture<Long> initPruningAsync(
            scala.collection.immutable.Set<UniqueAddress> nodes,
            Function<akka.cluster.ddata.replicator.PruningSupport.Param, Unit> func
    ) {
        final CompletableFuture<Long> promise = new CompletableFuture<>();
        AsyncExecutor.submit(() -> {
            Long r = initPruning(nodes, func);
            promise.complete(r);
            return r;
        });
        return promise;
    }

    private <Env> Long initPruning(
        scala.collection.immutable.Set<UniqueAddress> nodes,
        Function<akka.cluster.ddata.replicator.PruningSupport.Param, Unit> func
    ) {
        long i = 0L;
        for (int index = 0; index < capacity; index += 1) { //search by segments
            long currentPtr = mapBase + (long) index * 8;
            RWLock lock = locks[index & (CONCURRENCY_LEVEL - 1)].lockRead();
            try {
                for (long entry; (entry = unsafe.getAddress(currentPtr)) != 0; currentPtr = entry + NEXT_OFFSET) { //search inside a segment
                    Long key = keyAt(entry);
                    SharedMemoryValue value = valueAt(entry);
                    Env envelope = (Env) value.envelope;
                    //func.apply(new akka.cluster.ddata.replicator.PruningSupport.Param(nodes, Long.toHexString(key), envelope));
                    func.apply(new akka.cluster.ddata.replicator.PruningSupport.Param(nodes, key.toString(), envelope));
                    i++;
                }
            } finally {
                lock.unlockRead();
            }
        }
        return i;
    }

    public <Env> CompletableFuture<Integer> performPruningAsync(BiFunction<String, Env, Unit> func) {
        final CompletableFuture<Integer> promise = new CompletableFuture<>();
        AsyncExecutor.submit(() -> {
            promise.complete(performPruning(func));
            return 1;
        });
        return promise;

    }

    private <Env> Integer performPruning(BiFunction<String, Env, Unit> func) {
        for (int index = 0; index < capacity; index += 1) { //search by segments
            long currentPtr = mapBase + (long) index * 8;
            RWLock lock = locks[index & (CONCURRENCY_LEVEL - 1)].lockRead();
            try {
                for (long entry; (entry = unsafe.getAddress(currentPtr)) != 0; currentPtr = entry + NEXT_OFFSET) { //search inside a segment
                    Long key = keyAt(entry);
                    SharedMemoryValue value = valueAt(entry);
                    Env envelope = (Env) value.envelope;
                    //func.apply(Long.toHexString(key), envelope);
                    func.apply(key.toString(), envelope);
                }
            } finally {
                lock.unlockRead();
            }
        }
        return 1;
    }
}