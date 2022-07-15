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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.Session;
import com.facebook.presto.execution.ForQueryExecution;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelector;
import com.facebook.presto.failureDetector.FailureDetector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.NodePartitionMap;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.SplitSourceFactory;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.getConcurrentLifespansPerNode;
import static com.facebook.presto.SystemSessionProperties.getMaxTasksPerStage;
import static com.facebook.presto.SystemSessionProperties.getWriterMinSize;
import static com.facebook.presto.SystemSessionProperties.isOptimizedScaleWriterProducerBuffer;
import static com.facebook.presto.execution.SqlStageExecution.createSqlStageExecution;
import static com.facebook.presto.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static com.facebook.presto.execution.scheduler.TableWriteInfo.createTableWriteInfo;
import static com.facebook.presto.spi.ConnectorId.isInternalSystemConnector;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class SectionExecutionFactory
{
    private final Metadata metadata;
    private final NodePartitioningManager nodePartitioningManager;
    private final NodeTaskMap nodeTaskMap;
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutor;
    private final FailureDetector failureDetector;
    private final SplitSchedulerStats schedulerStats;
    private final NodeScheduler nodeScheduler;
    private final int splitBatchSize;

    @Inject
    public SectionExecutionFactory(
            Metadata metadata,
            NodePartitioningManager nodePartitioningManager,
            NodeTaskMap nodeTaskMap,
            @ForQueryExecution ExecutorService executor,
            @ForScheduler ScheduledExecutorService scheduledExecutor,
            FailureDetector failureDetector,
            SplitSchedulerStats schedulerStats,
            NodeScheduler nodeScheduler,
            QueryManagerConfig queryManagerConfig)
    {
        this(
                metadata,
                nodePartitioningManager,
                nodeTaskMap,
                executor,
                scheduledExecutor,
                failureDetector,
                schedulerStats,
                nodeScheduler,
                requireNonNull(queryManagerConfig, "queryManagerConfig is null").getScheduleSplitBatchSize());
    }

    public SectionExecutionFactory(
            Metadata metadata,
            NodePartitioningManager nodePartitioningManager,
            NodeTaskMap nodeTaskMap,
            ExecutorService executor,
            ScheduledExecutorService scheduledExecutor,
            FailureDetector failureDetector,
            SplitSchedulerStats schedulerStats,
            NodeScheduler nodeScheduler,
            int splitBatchSize)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
        this.splitBatchSize = splitBatchSize;
    }

    /**
     * returns a List of SectionExecutions in a postorder representation of the tree
     */
    public SectionExecution createSectionExecutions(
            Session session,
            StreamingPlanSection section,
            ExchangeLocationsConsumer locationsConsumer,
            Optional<int[]> bucketToPartition,
            OutputBuffers outputBuffers,
            boolean summarizeTaskInfo,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            int attemptId)
    {
        // Only fetch a distribution once per section to ensure all stages see the same machine assignments
        Map<PartitioningHandle, NodePartitionMap> partitioningCache = new HashMap<>();
        TableWriteInfo tableWriteInfo = createTableWriteInfo(section.getPlan(), metadata, session);
        // 重点入口不要忽略
        List<StageExecutionAndScheduler> sectionStages = createStreamingLinkedStageExecutions(
                session,
                locationsConsumer,
                section.getPlan().withBucketToPartition(bucketToPartition),
                partitioningHandle -> partitioningCache.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(session, handle)),
                tableWriteInfo,
                Optional.empty(),
                summarizeTaskInfo,
                remoteTaskFactory,
                splitSourceFactory,
                attemptId);
        // 将最后一个Stages作为rootStage
        StageExecutionAndScheduler rootStage = getLast(sectionStages);
        rootStage.getStageExecution().setOutputBuffers(outputBuffers);
        return new SectionExecution(rootStage, sectionStages);
    }

    /**
     * returns a List of StageExecutionAndSchedulers in a postorder representation of the tree
     */
    private List<StageExecutionAndScheduler> createStreamingLinkedStageExecutions(
            Session session,
            ExchangeLocationsConsumer parent,
            StreamingSubPlan plan,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            TableWriteInfo tableWriteInfo,
            Optional<SqlStageExecution> parentStageExecution,
            boolean summarizeTaskInfo,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            int attemptId)
    {
        ImmutableList.Builder<StageExecutionAndScheduler> stageExecutionAndSchedulers = ImmutableList.builder();
        // 获取PlanFragmentId
        PlanFragmentId fragmentId = plan.getFragment().getId();
        // 获取StageId
        StageId stageId = new StageId(session.getQueryId(), fragmentId.getId());
        SqlStageExecution stageExecution = createSqlStageExecution(
                new StageExecutionId(stageId, attemptId),
                plan.getFragment(),
                remoteTaskFactory,
                session,
                summarizeTaskInfo,
                nodeTaskMap,
                executor,
                failureDetector,
                schedulerStats,
                tableWriteInfo);

        PartitioningHandle partitioningHandle = plan.getFragment().getPartitioning();
        List<RemoteSourceNode> remoteSourceNodes = plan.getFragment().getRemoteSourceNodes();
        Optional<int[]> bucketToPartition = getBucketToPartition(partitioningHandle, partitioningCache, plan.getFragment().getRoot(), remoteSourceNodes);

        // create child stages
        ImmutableSet.Builder<SqlStageExecution> childStagesBuilder = ImmutableSet.builder();
        // 遍历Root Plan的各个子节点生成各个Plan对应的Stage
        for (StreamingSubPlan stagePlan : plan.getChildren()) {
            List<StageExecutionAndScheduler> subTree = createStreamingLinkedStageExecutions(
                    session,
                    stageExecution::addExchangeLocations,
                    stagePlan.withBucketToPartition(bucketToPartition),
                    partitioningCache,
                    tableWriteInfo,
                    Optional.of(stageExecution),
                    summarizeTaskInfo,
                    remoteTaskFactory,
                    splitSourceFactory,
                    attemptId);
            stageExecutionAndSchedulers.addAll(subTree);
            childStagesBuilder.add(getLast(subTree).getStageExecution());
        }
        Set<SqlStageExecution> childStageExecutions = childStagesBuilder.build();
        stageExecution.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                childStageExecutions.forEach(SqlStageExecution::cancel);
            }
        });

        StageLinkage stageLinkage = new StageLinkage(fragmentId, parent, childStageExecutions);
        /**
         * 创建Stage调度器
         */
        StageScheduler stageScheduler = createStageScheduler(
                splitSourceFactory,
                session,
                plan,
                partitioningCache,
                parentStageExecution,
                stageId,
                stageExecution,
                partitioningHandle,
                tableWriteInfo,
                childStageExecutions);
        stageExecutionAndSchedulers.add(new StageExecutionAndScheduler(
                stageExecution,
                stageLinkage,
                stageScheduler));

        return stageExecutionAndSchedulers.build();
    }

    /**
     * 创建Stage调度策略
     *
     * @param splitSourceFactory
     * @param session
     * @param plan
     * @param partitioningCache
     * @param parentStageExecution
     * @param stageId
     * @param stageExecution
     * @param partitioningHandle
     * @param tableWriteInfo
     * @param childStageExecutions
     * @return
     */
    private StageScheduler createStageScheduler(
            SplitSourceFactory splitSourceFactory,
            Session session,
            StreamingSubPlan plan,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            Optional<SqlStageExecution> parentStageExecution,
            StageId stageId,
            SqlStageExecution stageExecution,
            PartitioningHandle partitioningHandle,
            TableWriteInfo tableWriteInfo,
            Set<SqlStageExecution> childStageExecutions)
    {
        Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(plan.getFragment(), session, tableWriteInfo);
        int maxTasksPerStage = getMaxTasksPerStage(session);
        // 如果当前stage是读取分布式数据源阶段
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION)) {
            // nodes are selected dynamically based on the constraints of the splits and the system load
            Map.Entry<PlanNodeId, SplitSource> entry = getOnlyElement(splitSources.entrySet());
            PlanNodeId planNodeId = entry.getKey();
            SplitSource splitSource = entry.getValue();
            ConnectorId connectorId = splitSource.getConnectorId();
            if (isInternalSystemConnector(connectorId)) {
                connectorId = null;
            }
            // 创建节点选择器
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, connectorId, maxTasksPerStage);
            // 分配split策略的实现类，也就说split是怎么分配的
            SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeSelector, stageExecution::getAllTasks);

            checkArgument(!plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution());
            return newSourcePartitionedSchedulerAsStageScheduler(stageExecution, planNodeId, splitSource, placementPolicy, splitBatchSize);
        }
        /**
         * 当partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)时（scale_writers配置参数为true），也即需要把数据写出到外部存储时，选择创建ScaledWriterScheduler调度器。
         */
        else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            Supplier<Collection<TaskStatus>> sourceTasksProvider = () -> childStageExecutions.stream()
                    .map(SqlStageExecution::getAllTasks)
                    .flatMap(Collection::stream)
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            Supplier<Collection<TaskStatus>> writerTasksProvider = () -> stageExecution.getAllTasks().stream()
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());
            /**
             * 创建ScaledWriterScheduler调度器
             */
            ScaledWriterScheduler scheduler = new ScaledWriterScheduler(
                    stageExecution,
                    sourceTasksProvider,
                    writerTasksProvider,
                    nodeScheduler.createNodeSelector(session, null),
                    scheduledExecutor,
                    getWriterMinSize(session),
                    isOptimizedScaleWriterProducerBuffer(session));
            whenAllStages(childStageExecutions, StageExecutionState::isDone)
                    .addListener(scheduler::finish, directExecutor());
            return scheduler;
        }
        else {
            // 如果当前stage是FIXED/SINGLE阶段。
            /**
             * Single和Fixed Stage的节点选择比较简单，均为随机性选择，且调用方法为同一个：NodeSelector.selectRandomNodes。
             * Single Stage 只会随机地从所有存活节点列表中选择一个节点，
             * Fixed Stage 会随机选择参数hash_partition_count和max_tasks_per_stage两者的小值数量的节点。
             */
            // 当splitSources为空时，即该stage输入数据包含有本地数据源时，则创建FixedSourcePartitionedScheduler调度器：
            if (!splitSources.isEmpty()) {
                // contains local source
                List<PlanNodeId> schedulingOrder = plan.getFragment().getTableScanSchedulingOrder();
                ConnectorId connectorId = partitioningHandle.getConnectorId().orElseThrow(IllegalStateException::new);
                List<ConnectorPartitionHandle> connectorPartitionHandles;
                boolean groupedExecutionForStage = plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution();
                if (groupedExecutionForStage) {
                    connectorPartitionHandles = nodePartitioningManager.listPartitionHandles(session, partitioningHandle);
                    checkState(!ImmutableList.of(NOT_PARTITIONED).equals(connectorPartitionHandles));
                }
                else {
                    connectorPartitionHandles = ImmutableList.of(NOT_PARTITIONED);
                }
                // 对工作节点进行分桶，里面保存了split到节点之间的对应关系
                BucketNodeMap bucketNodeMap;
                List<InternalNode> stageNodeList;
                if (plan.getFragment().getRemoteSourceNodes().stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                    // no non-replicated remote source
                    boolean dynamicLifespanSchedule = plan.getFragment().getStageExecutionDescriptor().isDynamicLifespanSchedule();
                    // 包含副本的远程数据源
                    bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, partitioningHandle, dynamicLifespanSchedule);

                    // verify execution is consistent with planner's decision on dynamic lifespan schedule
                    verify(bucketNodeMap.isDynamic() == dynamicLifespanSchedule);

                    if (bucketNodeMap.hasInitialMap()) {
                        stageNodeList = bucketNodeMap.getBucketToNode().get().stream()
                                .distinct()
                                .collect(toImmutableList());
                    }
                    else {
                        // 随机选择maxTasksPerStage个节点
                        stageNodeList = new ArrayList<>(nodeScheduler.createNodeSelector(session, connectorId).selectRandomNodes(maxTasksPerStage));
                    }
                }
                else {
                    // cannot use dynamic lifespan schedule
                    verify(!plan.getFragment().getStageExecutionDescriptor().isDynamicLifespanSchedule());
                    // 为远程数据源获取nodePartitionMap
                    // remote source requires nodePartitionMap
                    NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());
                    if (groupedExecutionForStage) {
                        checkState(connectorPartitionHandles.size() == nodePartitionMap.getBucketToPartition().length);
                    }
                    stageNodeList = nodePartitionMap.getPartitionToNode();
                    bucketNodeMap = nodePartitionMap.asBucketNodeMap();
                }
                /**
                 * 从FixedSourcePartitionedScheduler的构造函数中可以看出，其对数据源的读取实际还是通过创建SourcePartitionedScheduler调度器来实现的。
                 */
                FixedSourcePartitionedScheduler fixedSourcePartitionedScheduler = new FixedSourcePartitionedScheduler(
                        stageExecution,
                        splitSources,
                        plan.getFragment().getStageExecutionDescriptor(),
                        schedulingOrder,
                        stageNodeList,
                        bucketNodeMap,
                        splitBatchSize,
                        getConcurrentLifespansPerNode(session),
                        nodeScheduler.createNodeSelector(session, connectorId),
                        connectorPartitionHandles);
                if (plan.getFragment().getStageExecutionDescriptor().isRecoverableGroupedExecution()) {
                    stageExecution.registerStageTaskRecoveryCallback(taskId -> {
                        checkArgument(taskId.getStageExecutionId().getStageId().equals(stageId), "The task did not execute this stage");
                        checkArgument(parentStageExecution.isPresent(), "Parent stage execution must exist");
                        checkArgument(parentStageExecution.get().getAllTasks().size() == 1, "Parent stage should only have one task for recoverable grouped execution");

                        parentStageExecution.get().removeRemoteSourceIfSingleTaskStage(taskId);
                        fixedSourcePartitionedScheduler.recover(taskId);
                    });
                }
                return fixedSourcePartitionedScheduler;
            }

            else {
                // 如果当前stage都是读取远程数据
                // all sources are remote
                NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());
                List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
                // todo this should asynchronously wait a standard timeout period before failing
                checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
                // FixedCountScheduler只是在nodePartitionMap.getPartitionToNode返回的固定节点列表上调度任务，在选定的每个节点上相应创建一个任务。
                return new FixedCountScheduler(stageExecution, partitionToNode);
            }
        }
    }

    private static Optional<int[]> getBucketToPartition(
            PartitioningHandle partitioningHandle,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            PlanNode fragmentRoot,
            List<RemoteSourceNode> remoteSourceNodes)
    {
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION) || partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            return Optional.of(new int[1]);
        }
        else if (PlanNodeSearcher.searchFrom(fragmentRoot).where(node -> node instanceof TableScanNode).findFirst().isPresent()) {
            if (remoteSourceNodes.stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                return Optional.empty();
            }
            else {
                // remote source requires nodePartitionMap
                NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
                return Optional.of(nodePartitionMap.getBucketToPartition());
            }
        }
        else {
            NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
            List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
            // todo this should asynchronously wait a standard timeout period before failing
            checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
            return Optional.of(nodePartitionMap.getBucketToPartition());
        }
    }

    private static ListenableFuture<?> whenAllStages(Collection<SqlStageExecution> stageExecutions, Predicate<StageExecutionState> predicate)
    {
        checkArgument(!stageExecutions.isEmpty(), "stageExecutions is empty");
        Set<StageExecutionId> stageIds = newConcurrentHashSet(stageExecutions.stream()
                .map(SqlStageExecution::getStageExecutionId)
                .collect(toSet()));
        SettableFuture<?> future = SettableFuture.create();

        for (SqlStageExecution stage : stageExecutions) {
            stage.addStateChangeListener(state -> {
                if (predicate.test(state) && stageIds.remove(stage.getStageExecutionId()) && stageIds.isEmpty()) {
                    future.set(null);
                }
            });
        }

        return future;
    }
}
