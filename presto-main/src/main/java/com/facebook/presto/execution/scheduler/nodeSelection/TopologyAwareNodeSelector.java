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
package com.facebook.presto.execution.scheduler.nodeSelection;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.scheduler.BucketNodeMap;
import com.facebook.presto.execution.scheduler.NetworkLocation;
import com.facebook.presto.execution.scheduler.NetworkLocationCache;
import com.facebook.presto.execution.scheduler.NodeAssignmentStats;
import com.facebook.presto.execution.scheduler.NodeMap;
import com.facebook.presto.execution.scheduler.NodeSelectionHashStrategy;
import com.facebook.presto.execution.scheduler.ResettableRandomizedIterator;
import com.facebook.presto.execution.scheduler.SplitPlacementResult;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SplitWeight;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.scheduler.NetworkLocation.ROOT_LOCATION;
import static com.facebook.presto.execution.scheduler.NodeScheduler.calculateLowWatermark;
import static com.facebook.presto.execution.scheduler.NodeScheduler.canAssignSplitBasedOnWeight;
import static com.facebook.presto.execution.scheduler.NodeScheduler.randomizedNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectDistributionNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectExactNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.toWhenHasSplitQueueSpaceFuture;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TopologyAwareNodeSelector
        implements NodeSelector
{
    private static final Logger log = Logger.get(TopologyAwareNodeSelector.class);

    private final InternalNodeManager nodeManager;
    private final NodeSelectionStats nodeSelectionStats;
    private final NodeTaskMap nodeTaskMap;
    private final boolean includeCoordinator;
    private final AtomicReference<Supplier<NodeMap>> nodeMap;
    private final int minCandidates;
    private final long maxSplitsWeightPerNode;
    private final long maxPendingSplitsWeightPerTask;
    private final int maxUnacknowledgedSplitsPerTask;
    private final List<CounterStat> topologicalSplitCounters;
    private final List<String> networkLocationSegmentNames;
    private final NetworkLocationCache networkLocationCache;
    private final NodeSelectionHashStrategy nodeSelectionHashStrategy;

    public TopologyAwareNodeSelector(
            InternalNodeManager nodeManager,
            NodeSelectionStats nodeSelectionStats,
            NodeTaskMap nodeTaskMap,
            boolean includeCoordinator,
            Supplier<NodeMap> nodeMap,
            int minCandidates,
            long maxSplitsWeightPerNode,
            long maxPendingSplitsWeightPerTask,
            int maxUnacknowledgedSplitsPerTask,
            List<CounterStat> topologicalSplitCounters,
            List<String> networkLocationSegmentNames,
            NetworkLocationCache networkLocationCache,
            NodeSelectionHashStrategy nodeSelectionHashStrategy)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.nodeSelectionStats = requireNonNull(nodeSelectionStats, "nodeSelectionStats is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.includeCoordinator = includeCoordinator;
        this.nodeMap = new AtomicReference<>(nodeMap);
        this.minCandidates = minCandidates;
        this.maxSplitsWeightPerNode = maxSplitsWeightPerNode;
        this.maxPendingSplitsWeightPerTask = maxPendingSplitsWeightPerTask;
        this.maxUnacknowledgedSplitsPerTask = maxUnacknowledgedSplitsPerTask;
        checkArgument(maxUnacknowledgedSplitsPerTask > 0, "maxUnacknowledgedSplitsPerTask must be > 0, found: %s", maxUnacknowledgedSplitsPerTask);
        this.topologicalSplitCounters = requireNonNull(topologicalSplitCounters, "topologicalSplitCounters is null");
        this.networkLocationSegmentNames = requireNonNull(networkLocationSegmentNames, "networkLocationSegmentNames is null");
        this.networkLocationCache = requireNonNull(networkLocationCache, "networkLocationCache is null");
        this.nodeSelectionHashStrategy = requireNonNull(nodeSelectionHashStrategy, "nodeSelectionHashStrategy is null");
    }

    @Override
    public void lockDownNodes()
    {
        nodeMap.set(Suppliers.ofInstance(nodeMap.get().get()));
    }

    @Override
    public List<InternalNode> getActiveNodes()
    {
        return ImmutableList.copyOf(nodeMap.get().get().getActiveNodes());
    }

    @Override
    public List<InternalNode> getAllNodes()
    {
        return ImmutableList.copyOf(nodeMap.get().get().getAllNodes());
    }

    @Override
    public InternalNode selectCurrentNode()
    {
        // TODO: this is a hack to force scheduling on the coordinator
        return nodeManager.getCurrentNode();
    }

    /**
     * 随机选择节点
     * @param limit
     * @param excludedNodes
     * @return
     */
    @Override
    public List<InternalNode> selectRandomNodes(int limit, Set<InternalNode> excludedNodes)
    {
        return selectNodes(limit, randomizedNodes(nodeMap.get().get(), includeCoordinator, excludedNodes));
    }

    /**
     * split 的分配策略
     * 1.将所有活跃的工作节点作为候选节点；
     * 2.如果分片的节点选择策略是HARD_AFFINITY，即分片只能在特定节点进行访问，则根据分片要求更新候选节点列表；
     * 3.如果分片的节点选择策略不是HARD_AFFINITY，则根据节点的网络拓扑，从候选节点中选择和分片偏好节点网络路径最匹配的节点列表来更新候选节点列表；
     * 4.使用bestNodeSplitCount方法从更新后的候选节点列表中选择最合适的节点来分配分片；
     */
    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks)
    {
        //拿出selector里面的nodeMap
        NodeMap nodeMap = this.nodeMap.get().get();
        // 存储每个节点分配的split，Multimap的特性是key可重复
        Multimap<InternalNode, Split> assignment = HashMultimap.create();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);

        int[] topologicCounters = new int[topologicalSplitCounters.size()];
        Set<NetworkLocation> filledLocations = new HashSet<>();
        // 阻塞的节点
        Set<InternalNode> blockedExactNodes = new HashSet<>();
        // 是否存在等待分配节点的split
        boolean splitWaitingForAnyNode = false;
        // 将所有活跃的节点作为候选节点
        NodeProvider nodeProvider = nodeMap.getActiveNodeProvider(nodeSelectionHashStrategy);

        for (Split split : splits) {
            // 如果分片的节点选择策略是HARD_AFFINITY，在连接器中进行设置
            SplitWeight splitWeight = split.getSplitWeight();
            if (split.getNodeSelectionStrategy() == HARD_AFFINITY) {
                // 从连接器的接口中获取该split的优先节点preferredNodes，以此作为候选节点
                List<InternalNode> candidateNodes = selectExactNodes(nodeMap, split.getPreferredNodes(nodeProvider), includeCoordinator);
                if (candidateNodes.isEmpty()) {
                    log.debug("No nodes available to schedule %s. Available nodes %s", split, nodeMap.getActiveNodes());
                    throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available to run query");
                }
                // 选择最合适的节点
                InternalNode chosenNode = bestNodeSplitCount(splitWeight, candidateNodes.iterator(), minCandidates, maxPendingSplitsWeightPerTask, assignmentStats);
                if (chosenNode != null) {
                    assignment.put(chosenNode, split);
                    assignmentStats.addAssignedSplit(chosenNode, splitWeight);
                }
                // 如果没有找到合适的节点并且有分片在等待某个节点，则将候选节点全部加入到阻塞节点列表中
                // Exact node set won't matter, if a split is waiting for any node
                else if (!splitWaitingForAnyNode) {
                    blockedExactNodes.addAll(candidateNodes);
                }
                continue;
            }

            InternalNode chosenNode = null;
            int depth = networkLocationSegmentNames.size();
            int chosenDepth = 0;
            // 表示网络拓扑中的一个位置。假定位置是分层的，并且所有工作节点和Split位置都应位于层次结构的同一级别。
            Set<NetworkLocation> locations = new HashSet<>();
            for (HostAddress host : split.getPreferredNodes(nodeProvider)) {
                locations.add(networkLocationCache.get(host));
            }
            if (locations.isEmpty()) {
                // Add the root location
                locations.add(ROOT_LOCATION);
                depth = 0;
            }
            // Try each address at progressively shallower network locations
            // 由深到浅地尝试每个地址的网络位置，也就是说更偏向于分配层次更浅的符合要求的节点
            for (int i = depth; i >= 0 && chosenNode == null; i--) {
                for (NetworkLocation location : locations) {
                    // Skip locations which are only shallower than this level
                    // For example, locations which couldn't be located will be at the "root" location
                    if (location.getSegments().size() < i) {
                        continue;
                    }
                    location = location.subLocation(0, i);
                    if (filledLocations.contains(location)) {
                        continue;
                    }
                    // 根据相应的网络路径获取活跃的工作节点
                    Set<InternalNode> nodes = nodeMap.getActiveWorkersByNetworkPath().get(location);
                    // 将活跃节点随机打散，并调用bestNodeSplitCount进行选择
                    /**
                     * 从候选节点列表中随机选择一个（传入节点列表时已进行随机打散new ResettableRandomizedIterator<>(nodes)），如果该节点已分配的Split尚未达到阈值，则选择该节点；
                     */
                    chosenNode = bestNodeSplitCount(splitWeight, new ResettableRandomizedIterator<>(nodes), minCandidates, calculateMaxPendingSplitsWeightPerTask(i, depth), assignmentStats);
                    if (chosenNode != null) {
                        chosenDepth = i;
                        break;
                    }
                    filledLocations.add(location);
                }
            }
            if (chosenNode != null) {
                //放入选择的node和对应的split
                assignment.put(chosenNode, split);
                assignmentStats.addAssignedSplit(chosenNode, splitWeight);
                topologicCounters[chosenDepth]++;
            }
            else {
                splitWaitingForAnyNode = true;
            }
        }
        for (int i = 0; i < topologicCounters.length; i++) {
            if (topologicCounters[i] > 0) {
                topologicalSplitCounters.get(i).update(topologicCounters[i]);
            }
        }

        ListenableFuture<?> blocked;
        long maxPendingForWildcardNetworkAffinity = calculateMaxPendingSplitsWeightPerTask(0, networkLocationSegmentNames.size());
        if (splitWaitingForAnyNode) {
            blocked = toWhenHasSplitQueueSpaceFuture(existingTasks, calculateLowWatermark(maxPendingForWildcardNetworkAffinity));
        }
        else {
            blocked = toWhenHasSplitQueueSpaceFuture(blockedExactNodes, existingTasks, calculateLowWatermark(maxPendingForWildcardNetworkAffinity));
        }
        return new SplitPlacementResult(blocked, assignment);
    }

    /**
     * Computes how much of the queue can be filled by splits with the network topology distance to a node given by
     * splitAffinity. A split with zero affinity can only fill half the queue, whereas one that matches
     * exactly can fill the entire queue.
     */
    private long calculateMaxPendingSplitsWeightPerTask(int splitAffinity, int totalDepth)
    {
        if (totalDepth == 0) {
            return maxPendingSplitsWeightPerTask;
        }
        // Use half the queue for any split
        // Reserve the other half for splits that have some amount of network affinity
        double queueFraction = 0.5 * (1.0 + splitAffinity / (double) totalDepth);
        return (long) Math.ceil(maxPendingSplitsWeightPerTask * queueFraction);
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, BucketNodeMap bucketNodeMap)
    {
        return selectDistributionNodes(nodeMap.get().get(), nodeTaskMap, maxSplitsWeightPerNode, maxPendingSplitsWeightPerTask, maxUnacknowledgedSplitsPerTask, splits, existingTasks, bucketNodeMap, nodeSelectionStats);
    }

    /**
     * 从更新后的候选节点列表中选择最合适的节点来分配分片；
     * 如果第1步选择的节点已分配Split达到上限，则选择剩余节点中在当前stage中排队Split最少的节点。
     *
     * @param splitWeight
     * @param candidates
     * @param minCandidatesWhenFull
     * @param maxPendingSplitsWeightPerTask
     * @param assignmentStats
     * @return
     */
    @Nullable
    private InternalNode bestNodeSplitCount(SplitWeight splitWeight, Iterator<InternalNode> candidates, int minCandidatesWhenFull, long maxPendingSplitsWeightPerTask, NodeAssignmentStats assignmentStats)
    {
        InternalNode bestQueueNotFull = null;
        long minWeight = Long.MAX_VALUE;
        int fullCandidatesConsidered = 0;

        while (candidates.hasNext() && (fullCandidatesConsidered < minCandidatesWhenFull || bestQueueNotFull == null)) {
            InternalNode node = candidates.next();
            if (assignmentStats.getUnacknowledgedSplitCountForStage(node) >= maxUnacknowledgedSplitsPerTask) {
                fullCandidatesConsidered++;
                continue;
            }
            if (canAssignSplitBasedOnWeight(assignmentStats.getTotalSplitsWeight(node), maxSplitsWeightPerNode, splitWeight)) {
                return node;
            }
            fullCandidatesConsidered++;
            long taskQueuedWeight = assignmentStats.getQueuedSplitsWeightForStage(node);
            if (taskQueuedWeight < minWeight && canAssignSplitBasedOnWeight(taskQueuedWeight, maxPendingSplitsWeightPerTask, splitWeight)) {
                minWeight = taskQueuedWeight;
                bestQueueNotFull = node;
            }
        }
        return bestQueueNotFull;
    }
}
