/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.hive.StoragePartitionLoader.BucketSplitInfo;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.util.ResumableTask;
import com.facebook.presto.hive.util.ResumableTasks;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class BackgroundHiveSplitLoader
        implements HiveSplitLoader
{
    private static final ListenableFuture<?> COMPLETED_FUTURE = immediateFuture(null);

    private final int loaderConcurrency;
    private final Executor executor;
    private final ConcurrentLazyQueue<HivePartitionMetadata> partitions;
    private final Deque<Iterator<InternalHiveSplit>> fileIterators = new ConcurrentLinkedDeque<>();
    private final PartitionLoader delegatingPartitionLoader;

    // Purpose of this lock:
    // * Write lock: when you need a consistent view across partitions, fileIterators, and hiveSplitSource.
    // * Read lock: when you need to modify any of the above.
    //   Make sure the lock is held throughout the period during which they may not be consistent with each other.
    // Details:
    // * When write lock is acquired, except the holder, no one can do any of the following:
    // ** poll from (or check empty) partitions
    // ** poll from (or check empty) or push to fileIterators
    // ** push to hiveSplitSource
    // * When any of the above three operations is carried out, either a read lock or a write lock must be held.
    // * When a series of operations involving two or more of the above three operations are carried out, the lock
    //   must be continuously held throughout the series of operations.
    // Implications:
    // * if you hold a read lock but not a write lock, you can do any of the above three operations, but you may
    //   see a series of operations involving two or more of the operations carried out half way.
    private final ReentrantReadWriteLock taskExecutionLock = new ReentrantReadWriteLock();

    private HiveSplitSource hiveSplitSource;
    private volatile boolean stopped;

    public BackgroundHiveSplitLoader(
            Table table,
            Iterable<HivePartitionMetadata> partitions,
            Optional<Domain> pathDomain,
            Optional<BucketSplitInfo> tableBucketInfo,
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            NamenodeStats namenodeStats,
            DirectoryLister directoryLister,
            Executor executor,
            int loaderConcurrency,
            boolean recursiveDirWalkerEnabled,
            boolean schedulerUsesHostAddresses,
            boolean partialAggregationsPushedDown)
    {
        this.loaderConcurrency = loaderConcurrency;
        checkArgument(loaderConcurrency > 0, "loaderConcurrency must be > 0, found: %s", loaderConcurrency);
        this.executor = requireNonNull(executor, "executor is null");
        this.partitions = new ConcurrentLazyQueue<>(requireNonNull(partitions, "partitions is null"));
        this.delegatingPartitionLoader = new DelegatingPartitionLoader(table, pathDomain, tableBucketInfo, session, hdfsEnvironment, namenodeStats, directoryLister, fileIterators, recursiveDirWalkerEnabled, schedulerUsesHostAddresses, partialAggregationsPushedDown);
    }

    @Override
    public void start(HiveSplitSource splitSource)
    {
        this.hiveSplitSource = splitSource;
        for (int i = 0; i < loaderConcurrency; i++) {
            ResumableTasks.submit(executor, new HiveSplitLoaderTask());
        }
    }

    @Override
    public void stop()
    {
        stopped = true;
    }

    private class HiveSplitLoaderTask
            implements ResumableTask
    {
        @Override
        public TaskStatus process()
        {
            while (true) {
                if (stopped) {
                    return TaskStatus.finished();
                }
                ListenableFuture<?> future;
                taskExecutionLock.readLock().lock();
                try {
                    // loadSplits()方法来不断加载splits。
                    future = loadSplits();
                }
                catch (Exception e) {
                    if (e instanceof IOException) {
                        e = new PrestoException(HIVE_FILESYSTEM_ERROR, e);
                    }
                    else if (!(e instanceof PrestoException)) {
                        e = new PrestoException(HIVE_UNKNOWN_ERROR, e);
                    }
                    // Fail the split source before releasing the execution lock
                    // Otherwise, a race could occur where the split source is completed before we fail it.
                    hiveSplitSource.fail(e);
                    checkState(stopped);
                    return TaskStatus.finished();
                }
                finally {
                    taskExecutionLock.readLock().unlock();
                }
                invokeNoMoreSplitsIfNecessary();
                if (!future.isDone()) {
                    return TaskStatus.continueOn(future);
                }
            }
        }
    }

    private void invokeNoMoreSplitsIfNecessary()
    {
        taskExecutionLock.readLock().lock();
        try {
            // This is an opportunistic check to avoid getting the write lock unnecessarily
            if (!partitions.isEmpty() || !fileIterators.isEmpty()) {
                return;
            }
        }
        catch (Exception e) {
            hiveSplitSource.fail(e);
            checkState(stopped, "Task is not marked as stopped even though it failed");
            return;
        }
        finally {
            taskExecutionLock.readLock().unlock();
        }

        taskExecutionLock.writeLock().lock();
        try {
            // the write lock guarantees that no one is operating on the partitions, fileIterators, or hiveSplitSource, or half way through doing so.
            if (partitions.isEmpty() && fileIterators.isEmpty()) {
                // It is legal to call `noMoreSplits` multiple times or after `stop` was called.
                // Nothing bad will happen if `noMoreSplits` implementation calls methods that will try to obtain a read lock because the lock is re-entrant.
                hiveSplitSource.noMoreSplits();
            }
        }
        catch (Exception e) {
            hiveSplitSource.fail(e);
            checkState(stopped, "Task is not marked as stopped even though it failed");
        }
        finally {
            taskExecutionLock.writeLock().unlock();
        }
    }

    private ListenableFuture<?> loadSplits()
            throws IOException
    {
        //获取等待处理的split信息
        Iterator<InternalHiveSplit> splits = fileIterators.poll();
        if (splits == null) { //如果当前没有需要处理的splits
            //尝试获取一个新的partiton进行处理
            HivePartitionMetadata partition = partitions.poll();
            if (partition == null) {
                return COMPLETED_FUTURE;
            }
            //加载这个partition，即读取HDFS,将这个partition的文件转化成一个一个的split
            /**
             *  1. presto先扫描所有所有需要访问的hdfs的数据文件，如果hdfs文件比hive.max-split-size(默认64M) 大，则一个文件生成一个split.
             *     其代码实现在于BackgroundHiveSplitLoader::loadSplits中，loadSplits会扫描分区的所有文件，每个文件创建一个InternalHiveSplit，提交到HiveSplitSource中异步生成真正的HiveSplit。
             *
             *  2. 在HiveSplitSource中，如果文件不可切割的话，则无论文件大大小多大都只生成一个split，如果可以切割而且文件大于hive.max-split-size，则对文件进行切割成多个split，每个split最大处理hive.max-split-size大小的数据
             */
            return delegatingPartitionLoader.loadPartition(partition, hiveSplitSource, stopped);
        }

        //开始遍历每一个splits
        while (splits.hasNext() && !stopped) {
            ListenableFuture<?> future = hiveSplitSource.addToQueue(splits.next());
            // 如果我们发现future 不是done的状态，证明hiveSplitSource出现了队列满等可能的异常，因此需要把这个splits重新放回fileIterators中
            if (!future.isDone()) {
                fileIterators.addFirst(splits);
                return future;
            }
        }

        // No need to put the iterator back, since it's either empty or we've stopped
        return COMPLETED_FUTURE;
    }
}
