/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.index.sbtreebonsai.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import com.orientechnologies.common.collection.OAlwaysGreaterKey;
import com.orientechnologies.common.collection.OAlwaysLessKey;
import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCachePointer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTree;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTreeException;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OStorageTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

/**
 * Tree-based dictionary algorithm. Similar to {@link OSBTree} but uses subpages of disk cache that is more efficient for small data
 * structures.
 * 
 * Oriented for usage of several instances inside of one file.
 * 
 * Creation of several instances that represent the same collection is not allowed.
 * 
 * @author Andrey Lomakin
 * @author Artem Orobets
 * @since 1.6.0
 * @see OSBTree
 */
public class OSBTreeBonsai<K, V> extends ODurableComponent {
  private static final OAlwaysLessKey    ALWAYS_LESS_KEY    = new OAlwaysLessKey();
  private static final OAlwaysGreaterKey ALWAYS_GREATER_KEY = new OAlwaysGreaterKey();

  private OBonsaiBucketPointer           rootBucketPointer;

  private final Comparator<? super K>    comparator         = ODefaultComparator.INSTANCE;

  private OStorageLocalAbstract          storage;
  private String                         name;

  private final String                   dataFileExtension;

  private ODiskCache                     diskCache;

  private long                           fileId;

  private int                            keySize;

  private OBinarySerializer<K>           keySerializer;
  private OBinarySerializer<V>           valueSerializer;

  private final boolean                  durableInNonTxMode;

  public OSBTreeBonsai(String dataFileExtension, int keySize, boolean durableInNonTxMode) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
    this.dataFileExtension = dataFileExtension;
    this.keySize = keySize;
    this.durableInNonTxMode = durableInNonTxMode;
  }

  public void create(String name, long rootIndex, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer,
      OStorageLocalAbstract storageLocal) {
    acquireExclusiveLock();
    try {
      this.storage = storageLocal;

      this.diskCache = storage.getDiskCache();

      this.name = name;
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;

      fileId = diskCache.openFile(name + dataFileExtension);

      initDurableComponent(storageLocal);

      OCacheEntry rootCacheEntry;
      if (rootIndex < 0)
        rootCacheEntry = diskCache.allocateNewPage(fileId);
      else
        rootCacheEntry = diskCache.load(fileId, rootIndex, false);

      // TODO
      rootBucketPointer = new OBonsaiBucketPointer(rootCacheEntry.getPageIndex(), 8192 * 2);

      OCachePointer rootPointer = rootCacheEntry.getCachePointer();

      rootPointer.acquireExclusiveLock();
      try {
        super.startDurableOperation(null);

        OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootPointer.getDataPointer(),
            rootBucketPointer.getPageOffset(), true, keySerializer, valueSerializer, getTrackMode());
        rootBucket.setKeySerializerId(keySerializer.getId());
        rootBucket.setValueSerializerId(valueSerializer.getId());
        rootBucket.setTreeSize(0);

        super.logPageChanges(rootBucket, fileId, rootBucketPointer.getPageIndex(), true);
        rootCacheEntry.markDirty();
      } finally {
        rootPointer.releaseExclusiveLock();
        diskCache.release(rootCacheEntry);
      }

      super.endDurableOperation(null, false);
    } catch (IOException e) {
      try {
        super.endDurableOperation(null, true);
      } catch (IOException e1) {
        OLogManager.instance().error(this, "Error during sbtree data rollback", e1);
      }
      throw new OSBTreeException("Error creation of sbtree with name" + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void initDurableComponent(OStorageLocalAbstract storageLocal) {
    OWriteAheadLog writeAheadLog = storageLocal.getWALInstance();
    init(writeAheadLog);
  }

  public String getName() {
    acquireSharedLock();
    try {
      return name;
    } finally {
      releaseSharedLock();
    }
  }

  public OBonsaiBucketPointer getRootBucketPointer() {
    acquireSharedLock();
    try {
      return rootBucketPointer;
    } finally {
      releaseSharedLock();
    }
  }

  public V get(K key) {
    acquireSharedLock();
    try {
      BucketSearchResult bucketSearchResult = findBucket(key, PartialSearchMode.NONE);
      if (bucketSearchResult.itemIndex < 0)
        return null;

      OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

      OCacheEntry keyBucketCacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      OCachePointer keyBucketPointer = keyBucketCacheEntry.getCachePointer();
      try {
        OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketPointer.getDataPointer(),
            bucketPointer.getPageOffset(), keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
        return keyBucket.getEntry(bucketSearchResult.itemIndex).value;
      } finally {
        diskCache.release(keyBucketCacheEntry);
      }

    } catch (IOException e) {
      throw new OSBTreeException("Error during retrieving  of sbtree with name " + name, e);
    } finally {
      releaseSharedLock();
    }
  }

  public void put(K key, V value) {
    acquireExclusiveLock();
    final OStorageTransaction transaction = storage.getStorageTransaction();
    try {
      startDurableOperation(transaction);

      BucketSearchResult bucketSearchResult = findBucket(key, PartialSearchMode.NONE);
      OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

      OCacheEntry keyBucketCacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      OCachePointer keyBucketPointer = keyBucketCacheEntry.getCachePointer();

      keyBucketPointer.acquireExclusiveLock();
      OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketPointer.getDataPointer(),
          bucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());

      if (bucketSearchResult.itemIndex >= 0) {
        while (!keyBucket.updateValue(bucketSearchResult.itemIndex, value)) {
          keyBucketPointer.releaseExclusiveLock();
          diskCache.release(keyBucketCacheEntry);

          bucketSearchResult = splitBucket(bucketSearchResult.path, bucketSearchResult.itemIndex, key);
          bucketPointer = bucketSearchResult.getLastPathItem();

          keyBucketCacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
          keyBucketPointer = keyBucketCacheEntry.getCachePointer();
          keyBucketPointer.acquireExclusiveLock();

          keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketPointer.getDataPointer(), bucketPointer.getPageOffset(),
              keySerializer, valueSerializer, getTrackMode());
        }

        logPageChanges(keyBucket, fileId, bucketSearchResult.getLastPathItem().getPageIndex(), false);
      } else {
        int insertionIndex = -bucketSearchResult.itemIndex - 1;

        while (!keyBucket.addEntry(insertionIndex, new OSBTreeBonsaiBucket.SBTreeEntry<K, V>(OBonsaiBucketPointer.NULL,
            OBonsaiBucketPointer.NULL, key, value), true)) {
          keyBucketPointer.releaseExclusiveLock();
          diskCache.release(keyBucketCacheEntry);

          bucketSearchResult = splitBucket(bucketSearchResult.path, insertionIndex, key);
          bucketPointer = bucketSearchResult.getLastPathItem();

          insertionIndex = bucketSearchResult.itemIndex;

          keyBucketCacheEntry = diskCache.load(fileId, bucketSearchResult.getLastPathItem().getPageIndex(), false);
          keyBucketPointer = keyBucketCacheEntry.getCachePointer();
          keyBucketPointer.acquireExclusiveLock();

          keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketPointer.getDataPointer(), bucketPointer.getPageOffset(),
              keySerializer, valueSerializer, getTrackMode());
        }

        logPageChanges(keyBucket, fileId, bucketPointer.getPageIndex(), false);
      }

      keyBucketCacheEntry.markDirty();
      keyBucketPointer.releaseExclusiveLock();
      diskCache.release(keyBucketCacheEntry);

      if (bucketSearchResult.itemIndex < 0)
        setSize(size() + 1);

      endDurableOperation(transaction, false);
    } catch (IOException e) {
      rollback(transaction);
      throw new OSBTreeException("Error during index update with key " + key + " and value " + value, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void rollback(OStorageTransaction transaction) {
    try {
      endDurableOperation(transaction, true);
    } catch (IOException e1) {
      OLogManager.instance().error(this, "Error during sbtree operation  rollback", e1);
    }
  }

  public void close(boolean flush) {
    acquireExclusiveLock();
    try {
      diskCache.closeFile(fileId, flush);
    } catch (IOException e) {
      throw new OSBTreeException("Error during close of index " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void close() {
    close(true);
  }

  // TODO don't clear whole file
  public void clear() {
    acquireExclusiveLock();
    OStorageTransaction transaction = storage.getStorageTransaction();
    try {
      startDurableOperation(transaction);

      diskCache.truncateFile(fileId);

      OCacheEntry cacheEntry = diskCache.load(fileId, rootBucketPointer.getPageIndex(), false);
      OCachePointer rootPointer = cacheEntry.getCachePointer();
      rootPointer.acquireExclusiveLock();
      try {
        OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootPointer.getDataPointer(),
            rootBucketPointer.getPageOffset(), true, keySerializer, valueSerializer, getTrackMode());

        rootBucket.setKeySerializerId(keySerializer.getId());
        rootBucket.setValueSerializerId(valueSerializer.getId());
        rootBucket.setTreeSize(0);

        logPageChanges(rootBucket, fileId, rootBucketPointer.getPageIndex(), true);
        cacheEntry.markDirty();
      } finally {
        rootPointer.releaseExclusiveLock();
        diskCache.release(cacheEntry);
      }

      endDurableOperation(transaction, false);
    } catch (IOException e) {
      rollback(transaction);

      throw new OSBTreeException("Error during clear of sbtree with name " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void delete() {
    acquireExclusiveLock();
    try {
      diskCache.deleteFile(fileId);
    } catch (IOException e) {
      throw new OSBTreeException("Error during delete of sbtree with name " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void load(String name, OBonsaiBucketPointer rootBucketPointer, OStorageLocalAbstract storageLocal) {
    acquireExclusiveLock();
    try {
      this.storage = storageLocal;
      this.rootBucketPointer = rootBucketPointer;

      diskCache = storage.getDiskCache();

      this.name = name;

      fileId = diskCache.openFile(name + dataFileExtension);

      OCacheEntry rootCacheEntry = diskCache.load(fileId, this.rootBucketPointer.getPageIndex(), false);
      OCachePointer rootPointer = rootCacheEntry.getCachePointer();
      try {
        OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootPointer.getDataPointer(),
            this.rootBucketPointer.getPageOffset(), keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
        keySerializer = (OBinarySerializer<K>) OBinarySerializerFactory.INSTANCE.getObjectSerializer(rootBucket
            .getKeySerializerId());
        valueSerializer = (OBinarySerializer<V>) OBinarySerializerFactory.INSTANCE.getObjectSerializer(rootBucket
            .getValueSerializerId());
      } finally {
        diskCache.release(rootCacheEntry);
      }
    } catch (IOException e) {
      throw new OSBTreeException("Exception during loading of sbtree " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void setSize(long size) throws IOException {
    OCacheEntry rootCacheEntry = diskCache.load(fileId, rootBucketPointer.getPageIndex(), false);

    OCachePointer rootPointer = rootCacheEntry.getCachePointer();
    rootPointer.acquireExclusiveLock();
    try {
      OSBTreeBonsaiBucket<K, V> rootBucket = new OSBTreeBonsaiBucket<K, V>(rootPointer.getDataPointer(),
          rootBucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());
      rootBucket.setTreeSize(size);

      logPageChanges(rootBucket, fileId, rootBucketPointer.getPageIndex(), false);
      rootCacheEntry.markDirty();
    } finally {
      rootPointer.releaseExclusiveLock();
      diskCache.release(rootCacheEntry);
    }
  }

  public long size() {
    acquireSharedLock();
    try {
      OCacheEntry rootCacheEntry = diskCache.load(fileId, rootBucketPointer.getPageIndex(), false);
      OCachePointer rootPointer = rootCacheEntry.getCachePointer();

      try {
        OSBTreeBonsaiBucket rootBucket = new OSBTreeBonsaiBucket<K, V>(rootPointer.getDataPointer(),
            rootBucketPointer.getPageOffset(), keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
        return rootBucket.getTreeSize();
      } finally {
        diskCache.release(rootCacheEntry);
      }
    } catch (IOException e) {
      throw new OSBTreeException("Error during retrieving of size of index " + name);
    } finally {
      releaseSharedLock();
    }
  }

  public V remove(K key) {
    acquireExclusiveLock();
    OStorageTransaction transaction = storage.getStorageTransaction();
    try {
      startDurableOperation(transaction);

      BucketSearchResult bucketSearchResult = findBucket(key, PartialSearchMode.NONE);
      if (bucketSearchResult.itemIndex < 0)
        return null;

      OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

      OCacheEntry keyBucketCacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      OCachePointer keyBucketPointer = keyBucketCacheEntry.getCachePointer();

      keyBucketPointer.acquireExclusiveLock();
      try {
        OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(keyBucketPointer.getDataPointer(),
            bucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());

        final V removed = keyBucket.getEntry(bucketSearchResult.itemIndex).value;

        keyBucket.remove(bucketSearchResult.itemIndex);

        logPageChanges(keyBucket, fileId, keyBucketCacheEntry.getPageIndex(), false);
        keyBucketCacheEntry.markDirty();

        endDurableOperation(transaction, false);
        return removed;
      } finally {
        keyBucketPointer.releaseExclusiveLock();
        diskCache.release(keyBucketCacheEntry);
        setSize(size() - 1);
      }

    } catch (IOException e) {
      rollback(transaction);

      throw new OSBTreeException("Error during removing key " + key + " from sbtree " + name, e);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  protected void endDurableOperation(OStorageTransaction transaction, boolean rollback) throws IOException {
    if (transaction == null && !durableInNonTxMode)
      return;

    super.endDurableOperation(transaction, rollback);
  }

  @Override
  protected void startDurableOperation(OStorageTransaction transaction) throws IOException {
    if (transaction == null && !durableInNonTxMode)
      return;

    super.startDurableOperation(transaction);
  }

  @Override
  protected void logPageChanges(ODurablePage localPage, long fileId, long pageIndex, boolean isNewPage) throws IOException {
    final OStorageTransaction transaction = storage.getStorageTransaction();
    if (transaction == null && !durableInNonTxMode)
      return;

    super.logPageChanges(localPage, fileId, pageIndex, isNewPage);
  }

  @Override
  protected ODurablePage.TrackMode getTrackMode() {
    final OStorageTransaction transaction = storage.getStorageTransaction();
    if (transaction == null && !durableInNonTxMode)
      return ODurablePage.TrackMode.NONE;

    return super.getTrackMode();
  }

  public Collection<V> getValuesMinor(K key, boolean inclusive, final int maxValuesToFetch) {
    final List<V> result = new ArrayList<V>();

    loadEntriesMinor(key, inclusive, new RangeResultListener<K, V>() {
      @Override
      public boolean addResult(OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry) {
        result.add(entry.value);
        if (maxValuesToFetch > -1 && result.size() >= maxValuesToFetch)
          return false;

        return true;
      }
    });

    return result;
  }

  public void loadEntriesMinor(K key, boolean inclusive, RangeResultListener<K, V> listener) {
    acquireSharedLock();
    try {
      final PartialSearchMode partialSearchMode;
      if (inclusive)
        partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;
      else
        partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;

      BucketSearchResult bucketSearchResult = findBucket(key, partialSearchMode);

      OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();
      int index;
      if (bucketSearchResult.itemIndex >= 0) {
        index = inclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex - 1;
      } else {
        index = -bucketSearchResult.itemIndex - 2;
      }

      boolean firstBucket = true;
      do {
        OCacheEntry cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
        final OCachePointer pointer = cacheEntry.getCachePointer();
        try {
          OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(pointer.getDataPointer(), bucketPointer.getPageOffset(),
              keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
          if (!firstBucket)
            index = bucket.size() - 1;

          for (int i = index; i >= 0; i--) {
            if (!listener.addResult(bucket.getEntry(i)))
              return;
          }

          bucketPointer = bucket.getLeftSibling();

          firstBucket = false;

        } finally {
          diskCache.release(cacheEntry);
        }
      } while (bucketPointer.getPageIndex() >= 0);
    } catch (IOException ioe) {
      throw new OSBTreeException("Error during fetch of minor values for key " + key + " in sbtree " + name);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<V> getValuesMajor(K key, boolean inclusive, final int maxValuesToFetch) {
    final List<V> result = new ArrayList<V>();

    loadEntriesMajor(key, inclusive, new RangeResultListener<K, V>() {
      @Override
      public boolean addResult(OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry) {
        result.add(entry.value);
        if (maxValuesToFetch > -1 && result.size() >= maxValuesToFetch)
          return false;

        return true;
      }
    });

    return result;
  }

  public void loadEntriesMajor(K key, boolean inclusive, RangeResultListener<K, V> listener) {
    acquireSharedLock();
    try {
      final PartialSearchMode partialSearchMode;
      if (inclusive)
        partialSearchMode = PartialSearchMode.LOWEST_BOUNDARY;
      else
        partialSearchMode = PartialSearchMode.HIGHEST_BOUNDARY;

      BucketSearchResult bucketSearchResult = findBucket(key, partialSearchMode);
      OBonsaiBucketPointer bucketPointer = bucketSearchResult.getLastPathItem();

      int index;
      if (bucketSearchResult.itemIndex >= 0) {
        index = inclusive ? bucketSearchResult.itemIndex : bucketSearchResult.itemIndex + 1;
      } else {
        index = -bucketSearchResult.itemIndex - 1;
      }

      do {
        final OCacheEntry cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
        final OCachePointer pointer = cacheEntry.getCachePointer();
        try {
          OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(pointer.getDataPointer(), bucketPointer.getPageOffset(),
              keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
          int bucketSize = bucket.size();
          for (int i = index; i < bucketSize; i++) {
            if (!listener.addResult(bucket.getEntry(i)))
              return;
          }

          bucketPointer = bucket.getRightSibling();
          index = 0;
        } finally {
          diskCache.release(cacheEntry);
        }

      } while (bucketPointer.getPageIndex() >= 0);

    } catch (IOException ioe) {
      throw new OSBTreeException("Error during fetch of major values for key " + key + " in sbtree " + name);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<V> getValuesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, final int maxValuesToFetch) {
    final List<V> result = new ArrayList<V>();
    loadEntriesBetween(keyFrom, fromInclusive, keyTo, toInclusive, new RangeResultListener<K, V>() {
      @Override
      public boolean addResult(OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry) {
        result.add(entry.value);
        if (maxValuesToFetch > 0 && result.size() >= maxValuesToFetch)
          return false;

        return true;
      }
    });

    return result;
  }

  public K firstKey() {
    acquireSharedLock();
    try {
      LinkedList<PagePathItemUnit> path = new LinkedList<PagePathItemUnit>();

      OBonsaiBucketPointer bucketPointer = rootBucketPointer;

      OCacheEntry cacheEntry = diskCache.load(fileId, rootBucketPointer.getPageIndex(), false);
      OCachePointer cachePointer = cacheEntry.getCachePointer();
      int itemIndex = 0;

      OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cachePointer.getDataPointer(),
          bucketPointer.getPageOffset(), keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
      try {
        while (true) {
          if (bucket.isLeaf()) {
            if (bucket.isEmpty()) {
              if (path.isEmpty()) {
                return null;
              } else {
                PagePathItemUnit pagePathItemUnit = path.removeLast();

                bucketPointer = pagePathItemUnit.bucketPointer;
                itemIndex = pagePathItemUnit.itemIndex + 1;
              }
            } else {
              return bucket.getKey(0);
            }
          } else {
            if (bucket.isEmpty() || itemIndex >= bucket.size()) {
              if (path.isEmpty()) {
                return null;
              } else {
                PagePathItemUnit pagePathItemUnit = path.removeLast();

                bucketPointer = pagePathItemUnit.bucketPointer;
                itemIndex = pagePathItemUnit.itemIndex + 1;
              }
            } else {
              OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry = bucket.getEntry(itemIndex);

              path.add(new PagePathItemUnit(bucketPointer, itemIndex));

              bucketPointer = entry.leftChild;
              itemIndex = 0;
            }
          }

          diskCache.release(cacheEntry);
          cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
          cachePointer = cacheEntry.getCachePointer();

          bucket = new OSBTreeBonsaiBucket<K, V>(cachePointer.getDataPointer(), bucketPointer.getPageOffset(), keySerializer,
              valueSerializer, ODurablePage.TrackMode.NONE);
        }
      } finally {
        diskCache.release(cacheEntry);
      }
    } catch (IOException e) {
      throw new OSBTreeException("Error during finding first key in sbtree [" + name + "]");
    } finally {
      releaseSharedLock();
    }
  }

  public K lastKey() {
    acquireSharedLock();
    try {
      LinkedList<PagePathItemUnit> path = new LinkedList<PagePathItemUnit>();

      OBonsaiBucketPointer bucketPointer = rootBucketPointer;

      OCacheEntry cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      OCachePointer cachePointer = cacheEntry.getCachePointer();
      OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(cachePointer.getDataPointer(),
          bucketPointer.getPageOffset(), keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);

      int itemIndex = bucket.size() - 1;
      try {
        while (true) {
          if (bucket.isLeaf()) {
            if (bucket.isEmpty()) {
              if (path.isEmpty()) {
                return null;
              } else {
                PagePathItemUnit pagePathItemUnit = path.removeLast();

                bucketPointer = pagePathItemUnit.bucketPointer;
                itemIndex = pagePathItemUnit.itemIndex - 1;
              }
            } else {
              return bucket.getKey(bucket.size() - 1);
            }
          } else {
            if (itemIndex < 0) {
              if (!path.isEmpty()) {
                PagePathItemUnit pagePathItemUnit = path.removeLast();

                bucketPointer = pagePathItemUnit.bucketPointer;
                itemIndex = pagePathItemUnit.itemIndex - 1;
              } else
                return null;
            } else {
              OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry = bucket.getEntry(itemIndex);

              path.add(new PagePathItemUnit(bucketPointer, itemIndex));

              bucketPointer = entry.rightChild;
              itemIndex = OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES + 1;
            }
          }

          diskCache.release(cacheEntry);
          cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
          cachePointer = cacheEntry.getCachePointer();

          bucket = new OSBTreeBonsaiBucket<K, V>(cachePointer.getDataPointer(), bucketPointer.getPageOffset(), keySerializer,
              valueSerializer, ODurablePage.TrackMode.NONE);
          if (itemIndex == OSBTreeBonsaiBucket.MAX_BUCKET_SIZE_BYTES + 1)
            itemIndex = bucket.size() - 1;
        }
      } finally {
        diskCache.release(cacheEntry);
      }
    } catch (IOException e) {
      throw new OSBTreeException("Error during finding first key in sbtree [" + name + "]");
    } finally {
      releaseSharedLock();
    }
  }

  public void loadEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, RangeResultListener<K, V> listener) {
    acquireSharedLock();
    try {
      PartialSearchMode partialSearchModeFrom;
      if (fromInclusive)
        partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
      else
        partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;

      BucketSearchResult bucketSearchResultFrom = findBucket(keyFrom, partialSearchModeFrom);

      OBonsaiBucketPointer bucketPointerFrom = bucketSearchResultFrom.getLastPathItem();

      int indexFrom;
      if (bucketSearchResultFrom.itemIndex >= 0) {
        indexFrom = fromInclusive ? bucketSearchResultFrom.itemIndex : bucketSearchResultFrom.itemIndex + 1;
      } else {
        indexFrom = -bucketSearchResultFrom.itemIndex - 1;
      }

      PartialSearchMode partialSearchModeTo;
      if (toInclusive)
        partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
      else
        partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;

      BucketSearchResult bucketSearchResultTo = findBucket(keyTo, partialSearchModeTo);
      OBonsaiBucketPointer bucketPointerTo = bucketSearchResultTo.getLastPathItem();

      int indexTo;
      if (bucketSearchResultTo.itemIndex >= 0) {
        indexTo = toInclusive ? bucketSearchResultTo.itemIndex : bucketSearchResultTo.itemIndex - 1;
      } else {
        indexTo = -bucketSearchResultTo.itemIndex - 2;
      }

      int startIndex = indexFrom;
      int endIndex;
      OBonsaiBucketPointer bucketPointer = bucketPointerFrom;

      resultsLoop: while (true) {

        final OCacheEntry cacheEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
        final OCachePointer pointer = cacheEntry.getCachePointer();
        try {
          OSBTreeBonsaiBucket<K, V> bucket = new OSBTreeBonsaiBucket<K, V>(pointer.getDataPointer(), bucketPointer.getPageOffset(),
              keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
          if (!bucketPointer.equals(bucketPointerTo))
            endIndex = bucket.size() - 1;
          else
            endIndex = indexTo;

          for (int i = startIndex; i <= endIndex; i++) {
            if (!listener.addResult(bucket.getEntry(i)))
              break resultsLoop;
          }

          if (bucketPointer.equals(bucketPointerTo))
            break;

          bucketPointer = bucket.getRightSibling();
          if (bucketPointer.getPageIndex() < 0)
            break;

        } finally {
          diskCache.release(cacheEntry);
        }

        startIndex = 0;
      }

    } catch (IOException ioe) {
      throw new OSBTreeException("Error during fetch of values between key " + keyFrom + " and key " + keyTo + " in sbtree " + name);
    } finally {
      releaseSharedLock();
    }
  }

  public void flush() {
    acquireSharedLock();
    try {
      try {
        diskCache.flushBuffer();
      } catch (IOException e) {
        throw new OSBTreeException("Error during flush of sbtree [" + name + "] data");
      }
    } finally {
      releaseSharedLock();
    }
  }

  private BucketSearchResult splitBucket(List<OBonsaiBucketPointer> path, int keyIndex, K keyToInsert) throws IOException {
    final OBonsaiBucketPointer bucketPointer = path.get(path.size() - 1);
    OCacheEntry bucketEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
    OCachePointer pointer = bucketEntry.getCachePointer();

    pointer.acquireExclusiveLock();
    try {
      OSBTreeBonsaiBucket<K, V> bucketToSplit = new OSBTreeBonsaiBucket<K, V>(pointer.getDataPointer(),
          bucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());

      final boolean splitLeaf = bucketToSplit.isLeaf();
      final int bucketSize = bucketToSplit.size();

      int indexToSplit = bucketSize >>> 1;
      final K separationKey = bucketToSplit.getKey(indexToSplit);
      final List<OSBTreeBonsaiBucket.SBTreeEntry<K, V>> rightEntries = new ArrayList<OSBTreeBonsaiBucket.SBTreeEntry<K, V>>(
          indexToSplit);

      final int startRightIndex = splitLeaf ? indexToSplit : indexToSplit + 1;

      for (int i = startRightIndex; i < bucketSize; i++)
        rightEntries.add(bucketToSplit.getEntry(i));

      if (!bucketPointer.equals(rootBucketPointer)) {
        // TODO
        OCacheEntry rightBucketEntry = diskCache.allocateNewPage(fileId);
        final OBonsaiBucketPointer rightBucketPointer = new OBonsaiBucketPointer(rightBucketEntry.getPageIndex(), 8192 * 2);

        OCachePointer rightPointer = rightBucketEntry.getCachePointer();

        rightPointer.acquireExclusiveLock();

        try {
          OSBTreeBonsaiBucket<K, V> newRightBucket = new OSBTreeBonsaiBucket<K, V>(rightPointer.getDataPointer(),
              rightBucketPointer.getPageOffset(), splitLeaf, keySerializer, valueSerializer, getTrackMode());
          newRightBucket.addAll(rightEntries);

          bucketToSplit.shrink(indexToSplit);

          if (splitLeaf) {
            OBonsaiBucketPointer rightSiblingBucketPointer = bucketToSplit.getRightSibling();

            newRightBucket.setRightSibling(rightSiblingBucketPointer);
            newRightBucket.setLeftSibling(bucketPointer);

            bucketToSplit.setRightSibling(rightBucketPointer);

            if (rightSiblingBucketPointer.isValid()) {
              final OCacheEntry rightSiblingBucketEntry = diskCache.load(fileId, rightSiblingBucketPointer.getPageIndex(), false);
              final OCachePointer rightSiblingPointer = rightSiblingBucketEntry.getCachePointer();

              rightSiblingPointer.acquireExclusiveLock();
              OSBTreeBonsaiBucket<K, V> rightSiblingBucket = new OSBTreeBonsaiBucket<K, V>(rightSiblingPointer.getDataPointer(),
                  rightSiblingBucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());
              try {
                rightSiblingBucket.setLeftSibling(rightBucketPointer);
                logPageChanges(rightSiblingBucket, fileId, rightSiblingBucketPointer.getPageIndex(), false);

                rightSiblingBucketEntry.markDirty();
              } finally {
                rightSiblingPointer.releaseExclusiveLock();
                diskCache.release(rightSiblingBucketEntry);
              }
            }
          }

          OBonsaiBucketPointer parentBucketPointer = path.get(path.size() - 2);
          OCacheEntry parentCacheEntry = diskCache.load(fileId, parentBucketPointer.getPageIndex(), false);
          OCachePointer parentPointer = parentCacheEntry.getCachePointer();

          parentPointer.acquireExclusiveLock();
          try {
            OSBTreeBonsaiBucket<K, V> parentBucket = new OSBTreeBonsaiBucket<K, V>(parentPointer.getDataPointer(),
                parentBucketPointer.getPageOffset(), keySerializer, valueSerializer, getTrackMode());
            OSBTreeBonsaiBucket.SBTreeEntry<K, V> parentEntry = new OSBTreeBonsaiBucket.SBTreeEntry<K, V>(bucketPointer,
                rightBucketPointer, separationKey, null);

            int insertionIndex = parentBucket.find(separationKey);
            assert insertionIndex < 0;

            insertionIndex = -insertionIndex - 1;
            while (!parentBucket.addEntry(insertionIndex, parentEntry, true)) {
              parentPointer.releaseExclusiveLock();
              diskCache.release(parentCacheEntry);

              BucketSearchResult bucketSearchResult = splitBucket(path.subList(0, path.size() - 1), insertionIndex, separationKey);

              parentBucketPointer = bucketSearchResult.getLastPathItem();
              parentCacheEntry = diskCache.load(fileId, parentBucketPointer.getPageIndex(), false);
              parentPointer = parentCacheEntry.getCachePointer();

              parentPointer.acquireExclusiveLock();

              insertionIndex = bucketSearchResult.itemIndex;

              parentBucket = new OSBTreeBonsaiBucket<K, V>(parentPointer.getDataPointer(), parentBucketPointer.getPageOffset(),
                  keySerializer, valueSerializer, getTrackMode());
            }

            logPageChanges(parentBucket, fileId, parentBucketPointer.getPageIndex(), false);
          } finally {
            parentCacheEntry.markDirty();
            parentPointer.releaseExclusiveLock();

            diskCache.release(parentCacheEntry);
          }

          logPageChanges(newRightBucket, fileId, rightBucketEntry.getPageIndex(), true);
        } finally {
          rightBucketEntry.markDirty();
          rightPointer.releaseExclusiveLock();
          diskCache.release(rightBucketEntry);
        }

        logPageChanges(bucketToSplit, fileId, bucketPointer.getPageIndex(), false);
        ArrayList<OBonsaiBucketPointer> resultPath = new ArrayList<OBonsaiBucketPointer>(path.subList(0, path.size() - 1));

        if (comparator.compare(keyToInsert, separationKey) < 0) {
          resultPath.add(bucketPointer);
          return new BucketSearchResult(keyIndex, resultPath);
        }

        resultPath.add(rightBucketPointer);
        if (splitLeaf) {
          return new BucketSearchResult(keyIndex - indexToSplit, resultPath);
        }
        return new BucketSearchResult(keyIndex - indexToSplit - 1, resultPath);

      } else {
        long treeSize = bucketToSplit.getTreeSize();

        final List<OSBTreeBonsaiBucket.SBTreeEntry<K, V>> leftEntries = new ArrayList<OSBTreeBonsaiBucket.SBTreeEntry<K, V>>(
            indexToSplit);

        for (int i = 0; i < indexToSplit; i++)
          leftEntries.add(bucketToSplit.getEntry(i));

        // TODO
        OCacheEntry leftBucketEntry = diskCache.allocateNewPage(fileId);
        OBonsaiBucketPointer leftBucketPointer = new OBonsaiBucketPointer(leftBucketEntry.getPageIndex(), 8192 * 2);
        OCachePointer leftPointer = leftBucketEntry.getCachePointer();

        // TODO
        OCacheEntry rightBucketEntry = diskCache.allocateNewPage(fileId);
        OBonsaiBucketPointer rightBucketPointer = new OBonsaiBucketPointer(rightBucketEntry.getPageIndex(), 8192 * 2);
        leftPointer.acquireExclusiveLock();
        try {
          OSBTreeBonsaiBucket<K, V> newLeftBucket = new OSBTreeBonsaiBucket<K, V>(leftPointer.getDataPointer(),
              leftBucketPointer.getPageOffset(), splitLeaf, keySerializer, valueSerializer, getTrackMode());
          newLeftBucket.addAll(leftEntries);

          if (splitLeaf)
            newLeftBucket.setRightSibling(rightBucketPointer);

          logPageChanges(newLeftBucket, fileId, leftBucketEntry.getPageIndex(), true);
          leftBucketEntry.markDirty();
        } finally {
          leftPointer.releaseExclusiveLock();
          diskCache.release(leftBucketEntry);
        }

        OCachePointer rightPointer = rightBucketEntry.getCachePointer();
        rightPointer.acquireExclusiveLock();
        try {
          OSBTreeBonsaiBucket<K, V> newRightBucket = new OSBTreeBonsaiBucket<K, V>(rightPointer.getDataPointer(),
              rightBucketPointer.getPageOffset(), splitLeaf, keySerializer, valueSerializer, getTrackMode());
          newRightBucket.addAll(rightEntries);

          if (splitLeaf)
            newRightBucket.setLeftSibling(leftBucketPointer);

          logPageChanges(newRightBucket, fileId, rightBucketEntry.getPageIndex(), true);
          rightBucketEntry.markDirty();
        } finally {
          rightPointer.releaseExclusiveLock();
          diskCache.release(rightBucketEntry);
        }

        bucketToSplit = new OSBTreeBonsaiBucket<K, V>(pointer.getDataPointer(), bucketPointer.getPageOffset(), false,
            keySerializer, valueSerializer, getTrackMode());
        bucketToSplit.setTreeSize(treeSize);

        bucketToSplit.addEntry(0, new OSBTreeBonsaiBucket.SBTreeEntry<K, V>(leftBucketPointer, rightBucketPointer, separationKey,
            null), true);

        logPageChanges(bucketToSplit, fileId, bucketPointer.getPageIndex(), false);
        ArrayList<OBonsaiBucketPointer> resultPath = new ArrayList<OBonsaiBucketPointer>(path.subList(0, path.size() - 1));

        if (comparator.compare(keyToInsert, separationKey) < 0) {
          resultPath.add(leftBucketPointer);
          return new BucketSearchResult(keyIndex, resultPath);
        }

        resultPath.add(rightBucketPointer);

        if (splitLeaf)
          return new BucketSearchResult(keyIndex - indexToSplit, resultPath);

        return new BucketSearchResult(keyIndex - indexToSplit - 1, resultPath);
      }

    } finally {
      bucketEntry.markDirty();
      pointer.releaseExclusiveLock();
      diskCache.release(bucketEntry);
    }
  }

  private BucketSearchResult findBucket(K key, PartialSearchMode partialSearchMode) throws IOException {
    OBonsaiBucketPointer bucketPointer = rootBucketPointer;
    final ArrayList<OBonsaiBucketPointer> path = new ArrayList<OBonsaiBucketPointer>();

    if (!(keySize == 1 || ((OCompositeKey) key).getKeys().size() == keySize || partialSearchMode.equals(PartialSearchMode.NONE))) {
      final OCompositeKey fullKey = new OCompositeKey((Comparable<? super K>) key);
      int itemsToAdd = keySize - fullKey.getKeys().size();

      final Comparable<?> keyItem;
      if (partialSearchMode.equals(PartialSearchMode.HIGHEST_BOUNDARY))
        keyItem = ALWAYS_GREATER_KEY;
      else
        keyItem = ALWAYS_LESS_KEY;

      for (int i = 0; i < itemsToAdd; i++)
        fullKey.addKey(keyItem);

      key = (K) fullKey;
    }

    while (true) {
      path.add(bucketPointer);
      final OCacheEntry bucketEntry = diskCache.load(fileId, bucketPointer.getPageIndex(), false);
      final OCachePointer pointer = bucketEntry.getCachePointer();

      final OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry;
      try {
        final OSBTreeBonsaiBucket<K, V> keyBucket = new OSBTreeBonsaiBucket<K, V>(pointer.getDataPointer(),
            bucketPointer.getPageOffset(), keySerializer, valueSerializer, ODurablePage.TrackMode.NONE);
        final int index = keyBucket.find(key);

        if (keyBucket.isLeaf())
          return new BucketSearchResult(index, path);

        if (index >= 0)
          entry = keyBucket.getEntry(index);
        else {
          final int insertionIndex = -index - 1;
          if (insertionIndex >= keyBucket.size())
            entry = keyBucket.getEntry(insertionIndex - 1);
          else
            entry = keyBucket.getEntry(insertionIndex);
        }

      } finally {
        diskCache.release(bucketEntry);
      }

      if (comparator.compare(key, entry.key) >= 0)
        bucketPointer = entry.rightChild;
      else
        bucketPointer = entry.leftChild;
    }
  }

  private static class BucketSearchResult {
    private final int                             itemIndex;
    private final ArrayList<OBonsaiBucketPointer> path;

    private BucketSearchResult(int itemIndex, ArrayList<OBonsaiBucketPointer> path) {
      this.itemIndex = itemIndex;
      this.path = path;
    }

    public OBonsaiBucketPointer getLastPathItem() {
      return path.get(path.size() - 1);
    }
  }

  /**
   * Indicates search behavior in case of {@link OCompositeKey} keys that have less amount of internal keys are used, whether lowest
   * or highest partially matched key should be used.
   * 
   * 
   */
  private static enum PartialSearchMode {
    /**
     * Any partially matched key will be used as search result.
     */
    NONE,
    /**
     * The biggest partially matched key will be used as search result.
     */
    HIGHEST_BOUNDARY,

    /**
     * The smallest partially matched key will be used as search result.
     */
    LOWEST_BOUNDARY
  }

  private static final class PagePathItemUnit {
    private final OBonsaiBucketPointer bucketPointer;
    private final int                  itemIndex;

    private PagePathItemUnit(OBonsaiBucketPointer bucketPointer, int itemIndex) {
      this.bucketPointer = bucketPointer;
      this.itemIndex = itemIndex;
    }
  }

  public static interface RangeResultListener<K, V> {
    public boolean addResult(OSBTreeBonsaiBucket.SBTreeEntry<K, V> entry);
  }

}