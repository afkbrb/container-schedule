package com.tianchi.django.common.utils;

import java.util.*;

import com.google.common.collect.*;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.Traverser;
import com.tianchi.django.common.enums.Resource;
import com.tianchi.django.common.pojo.*;
import com.tianchi.django.common.pojo.associate.*;
import com.tianchi.django.common.pojo.rule.ScoreWeight;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.*;
import org.apache.commons.lang3.tuple.ImmutablePair;

import static com.tianchi.django.common.constants.DjangoConstants.INVALID_SCORE;
import static com.tianchi.django.common.constants.DjangoConstants.SCORE_EMPTY_NODE_MODEL_NAME;
import static com.tianchi.django.common.utils.HCollectors.countingInteger;
import static com.tianchi.django.common.utils.HCollectors.entriesToMap;
import static java.util.stream.Collectors.*;

@Slf4j
public class ScoreUtils {

    /**
     * 根据规则计算单机使用资源的分数
     */
    public static int resourceScore(Node node, Rule rule) {
        return resourceScore(ImmutableList.of(node), rule);
    }

    /**
     * 根据规则计算多台宿主机使用资源的分数
     */
    public static int resourceScore(List<Node> nodes, Rule rule) {
        Map<Resource, Map<String, Integer>> scoreMap = RuleUtils.toResourceScoreMap(rule);
        int sum = nodes.parallelStream().mapToInt(node -> resourceScore(scoreMap, node)).sum();
        double avg = nodes.parallelStream().mapToInt(node -> resourceScore(scoreMap, node)).average().orElse(0);
        if (nodes.size() > 1) log.info("node needed: {}, totalScore: {}, avg: {}", nodes.size(), sum, avg);
        return nodes.parallelStream().mapToInt(node -> resourceScore(scoreMap, node)).sum();
    }

    /**
     * 根据规则计算多台宿主机静态布局分数
     */
    public static int scoreNodeWithPods(List<NodeWithPod> nodeWithPods, Rule rule, List<GroupRuleAssociate> groupRuleAssociates) {

        // 过滤掉没有被使用的 node
        nodeWithPods = ListUtils.emptyIfNull(nodeWithPods).parallelStream().filter(nwp -> CollectionUtils.isNotEmpty(nwp.getPods())).collect(toList());

        if (!dataSatisfy(nodeWithPods, groupRuleAssociates)) {
            return INVALID_SCORE;
        }

        if (!resourceSatisfy(nodeWithPods)) {
            return INVALID_SCORE;
        }

        int layoutScore = layoutScore(nodeWithPods, rule, groupRuleAssociates);

        if (layoutScore < 0) {
            return INVALID_SCORE;
        }

        int cgroupScore = cgroupScore(nodeWithPods, rule);

        if (cgroupScore < 0) {
            return INVALID_SCORE;
        }

        int resourceScore = resourceScore(nodeWithPods.parallelStream().map(NodeWithPod::getNode).collect(toList()), rule);

        log.info("layoutScore:{} , cgroupScore:{} , resourceScore:{} ", layoutScore, cgroupScore, resourceScore);

        return resourceScore + layoutScore + cgroupScore;

    }

    /**
     * 精细化cpu调度分数
     */
    private static int cgroupScore(List<NodeWithPod> nodeWithPods, Rule rule) {

        ScoreWeight sw = rule.getScoreWeight();

        int totalSocketCrossCount = 0, totalCoreBindCount = 0, totalSensitiveCpuBindCount = 0;

        for (NodeWithPod nwp : nodeWithPods) {

            Node node = nwp.getNode();

            //node中不存在topologies，不打分
            if (CollectionUtils.isEmpty(node.getTopologies())) {
                continue;
            }

            List<Pod> pods = ListUtils.emptyIfNull(nwp.getPods());

            //任何pod未分配cpu信息，当前布局无效。
            if (pods.stream().anyMatch(pod -> CollectionUtils.isEmpty(pod.getCpuIDs()))) {
                log.error("go against cgroup | node:{} have pod no cpuIDs", node.getSn());
                return INVALID_SCORE;
            }

            if (pods.stream().anyMatch(pod -> pod.getCpuIDs().size() != pod.getCpu())) {
                log.error("go against cgroup | node:{} have pod cpuID size unequal to pod cpu", node.getSn());
                return INVALID_SCORE;
            }

            Map<Integer, Integer> cpuIDCountAgainstMap = NodeWithPodUtils.cpuIDCountMap(nwp).entrySet().stream().filter(entry -> entry.getValue() > 1).collect(entriesToMap());

            //cpuid分配重复，当前布局无效
            if (MapUtils.isNotEmpty(cpuIDCountAgainstMap)) {
                log.error("go against cgroup | node:{} cpuIds overlap: {}", node.getSn(), JsonTools.toSimplifyJson(cpuIDCountAgainstMap));
                return INVALID_SCORE;
            }

            Map<Integer, Integer> cpuToSocket = NodeUtils.cpuToSocket(node);

            for (Pod pod : pods) {//校验是否存在cpu乱写的情况。
                for (int cpuId : pod.getCpuIDs()) {
                    Integer socket = cpuToSocket.get(cpuId);
                    if (socket == null) {//socket不存在表示此容器cpuId不是来自node的topology
                        log.error("go against cgroup | node:{} cpuId :{} invalid", node.getSn(), cpuId);
                        return INVALID_SCORE;
                    }
                }
            }

            for (Pod pod : pods) {
                //不同socket下对应的cpuId数量。
                Map<Integer, Integer> socketCountMap = pod.getCpuIDs().stream().collect(countingInteger(cpuToSocket::get));

                if (socketCountMap.size() > 1) {//出现跨socket现象

                    Map.Entry<Integer, Integer> maxCountSocketEntry = new AbstractMap.SimpleImmutableEntry<>(-1, -1);//找出不同socket下分配cpu最多的socket

                    for (Map.Entry<Integer, Integer> entry : socketCountMap.entrySet()) {
                        if (entry.getValue() > maxCountSocketEntry.getValue()) {
                            maxCountSocketEntry = entry;
                        }
                    }

                    socketCountMap.remove(maxCountSocketEntry.getKey());//过滤分配cpu最多的socket

                    totalSocketCrossCount += socketCountMap.values().stream().mapToInt($ -> $).sum();//剩余的则为跨socket的cpu数量

                }

            }

            Map<Integer, Integer> cpuToCore = NodeUtils.cpuToCore(node);

            for (Pod pod : pods) {

                //找出容器同core绑定信息<core,count>
                Map<Integer, Integer> sameCoreMap = pod.getCpuIDs().stream().collect(countingInteger(cpuToCore::get))
                    .entrySet().stream().filter(entry -> entry.getValue() > 1).collect(entriesToMap());

                for (Map.Entry<Integer, Integer> entry : sameCoreMap.entrySet()) {
                    totalCoreBindCount += entry.getValue() - 1;
                }

            }

            //TODO 已经很复杂了，如果排名拉不开差距在增加sensitiveCpuBind数据校验

        }

        log.info("socketCross: {}, coreBind: {}", totalSocketCrossCount, totalCoreBindCount);
        return totalSocketCrossCount * sw.getSocketCross() + totalCoreBindCount * sw.getCoreBind() + totalSensitiveCpuBindCount * sw.getSensitiveCpuBind();

    }

    /**
     * 校验结果数据是否与原始数据一致
     */
    private static boolean dataSatisfy(List<NodeWithPod> nodeWithPods, List<GroupRuleAssociate> groupRuleAssociates) {

        Map<String, Integer> sourceGroupReplicas = groupRuleAssociates.stream().collect(toMap(GroupRuleAssociate::getGroup, GroupRuleAssociate::getReplicas));

        Map<String, Integer> resultGroupReplicas = NodeWithPodUtils.toPods(nodeWithPods).stream().collect(HCollectors.countingInteger(Pod::getGroup));

        Set<String> sourceGroups = Sets.newHashSet(sourceGroupReplicas.keySet());

        if (Sets.difference(sourceGroups, resultGroupReplicas.keySet()).size() != 0) {//校验原始数据、结果数据分组数量是否一致
            return false;
        }

        //校验原始数据、结果数据不同分组下数量是否一致。
        return sourceGroups.parallelStream().allMatch(group -> Objects.equals(sourceGroupReplicas.get(group), resultGroupReplicas.get(group)));

    }

    /**
     * 校验结果数据是否超过资源约束
     */
    private static boolean resourceSatisfy(List<NodeWithPod> nodeWithPods) {

        for (NodeWithPod nwp : nodeWithPods) {

            Node node = nwp.getNode();

            List<Pod> pods = nwp.getPods();

            if (node.getEni() < pods.size()) {
                log.error("go against eni alloc | node:{} , eni: {} ,podSize: {}", node.getSn(), node.getEni(), pods.size());
                return false;
            }

            for (Resource resource : Resource.class.getEnumConstants()) {

                int nodeResource = node.value(resource);

                int podsResource = PodUtils.totalResource(pods, resource);

                if (nodeResource < podsResource) {
                    log.error("go against resource alloc | node:{} ,resource: {} ,nodeResource:{} , podsResource:{}",
                        node.getSn(), resource, nodeResource, podsResource);
                    return false;
                }
            }

        }

        return true;
    }

    /**
     * 容器静态布局分数
     */
    private static int layoutScore(List<NodeWithPod> nodeWithPods, Rule rule, List<GroupRuleAssociate> groupRuleAssociates) {

        int totalGroupMoreInstancePerNodeCount = 0;

        Map<String, Integer> maxInstancePerNodes = RuleUtils.toAllMaxInstancePerNodeLimit(rule, groupRuleAssociates);//所有应用分组对应的单机最大堆叠数量

        for (NodeWithPod nwp : nodeWithPods) {

            //当前宿主机上布局的应用分组数据<group,count>
            Map<String, Integer> groupCountPreNodeMap = NodeWithPodUtils.groupCountPreNodeMap(nwp);

            for (Map.Entry<String, Integer> entry : groupCountPreNodeMap.entrySet()) {

                if (entry.getValue() > maxInstancePerNodes.get(entry.getKey())) {//当前机器和应用分组超过允许堆叠的数量，当前布局无效。
                    log.error("go against layout | node:{} , group:{} , count:{} , maxInstancePerNode:{}",
                        nwp.getNode().getSn(), entry.getKey(), entry.getValue(), maxInstancePerNodes.get(entry.getKey()));
                    return INVALID_SCORE;
                }

                if (entry.getValue() > 1) {//当前宿主机下，若此应用分组布局多于一个。
                    totalGroupMoreInstancePerNodeCount += entry.getValue() - 1;
                }
            }
        }

        return totalGroupMoreInstancePerNodeCount * rule.getScoreWeight().getGroupMoreInstancePerNode();
    }

    private static int resourceScore(Map<Resource, Map<String, Integer>> scoreMap, Node node) {
        return Arrays.stream(Resource.class.getEnumConstants())
            .mapToInt(
                resource -> {
                    Map<String, Integer> map = scoreMap.getOrDefault(resource, Maps.newHashMap());
                    return map.getOrDefault(node.getNodeModelName(), map.getOrDefault(SCORE_EMPTY_NODE_MODEL_NAME, 0)) * node.value(resource);
                }
            ).sum();
    }

    public static int scoreReschedule(List<RescheduleResult> results, Rule rule, List<NodeWithPod> sourceList) {

        // 1. 根据迁移方案建立图。
        // 其实这个图作用不大，并没有用来检验环之类的。
        Set<Integer> stageSet = results.parallelStream().map(RescheduleResult::getStage).collect(toSet());

        int minStage = stageSet.stream().mapToInt($ -> $).min().orElse(0), maxStage = stageSet.stream().mapToInt($ -> $).max().orElse(0);

        if (minStage != 1 || stageSet.size() != maxStage) {
            return INVALID_SCORE;
        }

        int migrateCount = results.size();

        MutableGraph<GraphMigrateNode> graph = GraphUtils.to(results.size() * 2 + 1);

        Map<Integer, List<RescheduleResult>> stageResultMap = results.stream().collect(groupingBy(RescheduleResult::getStage));

        ImmutablePair<Integer, GraphMigrateNode> lastStagePair = ImmutablePair.of(0, GraphMigrateNode.ROOT);

        ImmutablePair<Integer, GraphMigrateNode> currentStagePair = ImmutablePair.of(0, GraphMigrateNode.ROOT);

        for (int stage = minStage; stage <= maxStage; stage++) {

            for (RescheduleResult result : stageResultMap.get(stage)) {

                // TODO：不允许自身内部迁移
                if (result.getSourceSn().equalsIgnoreCase(result.getTargetSn())) {
                    return INVALID_SCORE;
                }

                GraphMigrateNode expansion = GraphMigrateNode.toExpansion(result.getPodSn(), result.getTargetSn(), result.getCpuIDs());

                GraphMigrateNode offine = GraphMigrateNode.toOffline(result.getPodSn(), result.getSourceSn());

                if (currentStagePair.getLeft() != stage) {
                    lastStagePair = currentStagePair;
                }

                if (stage == lastStagePair.getLeft() + 1) {
                    graph.putEdge(lastStagePair.right, expansion);
                }

                graph.putEdge(expansion, offine);

                currentStagePair = ImmutablePair.of(stage, offine);

            }

        }

        // 2. 验证迁移策略是否可行并执行迁移
        List<NodeWithPod> nodeWithPods = sourceList.parallelStream().map(NodeWithPod::copy).collect(toList());

        Traverser<GraphMigrateNode> traverser = Traverser.forGraph(graph);

        List<GroupRuleAssociate> groupRuleAssociates = GroupRuleAssociate.fromPods(NodeWithPodUtils.toPods(nodeWithPods));

        Map<String, Integer> allMaxInstancePerNodeLimit = RuleUtils.toAllMaxInstancePerNodeLimit(rule, groupRuleAssociates);

        List<NodeWithPod> result = verifyAndTransformCluster(nodeWithPods, traverser, allMaxInstancePerNodeLimit);

        if (CollectionUtils.isEmpty(result)) {
            return INVALID_SCORE;
        }

        // 3. 根据迁移结果计算分数
        int migrateScore = migrateCount * rule.getScoreWeight().getMigratePod();

        int scheduleScore = scoreNodeWithPods(result, rule, groupRuleAssociates);

        log.info("migrate score:{}, and after migrate cluster schedule score:{}", migrateScore, scheduleScore);

        if (scheduleScore == INVALID_SCORE) {
            return INVALID_SCORE;
        }

        return migrateScore + scheduleScore;
    }

    /**
     * 根据图验证迁移方案是否可行，并返回迁移后的结果。
     */
    private static List<NodeWithPod> verifyAndTransformCluster(List<NodeWithPod> nodeWithPods, Traverser<GraphMigrateNode> traverser, Map<String, Integer> allMaxInstancePerNodeLimit) {

        Map<String, Pod> allPodMap = NodeWithPodUtils.toPodMap(nodeWithPods);

        for (GraphMigrateNode graphNode : traverser.breadthFirst(GraphMigrateNode.ROOT)) {

            if (GraphMigrateNode.isRoot(graphNode)) {
                continue;
            }

            // 找到 pod 原来所在的哪个 node
            NodeWithPod nodeWithPod = nodeWithPods.parallelStream().filter(nwp -> nwp.getNode().getSn().equals(graphNode.getNodeSn())).findFirst().orElse(null);

            if (nodeWithPod == null) {
                return Collections.emptyList();
            }

            // 找到 pod
            Pod verifyPod = allPodMap.get(graphNode.getPodSn());

            if (verifyPod == null) {
                return Collections.emptyList();
            }

            if (OperationType.EXPANSION.equals(graphNode.getType())) {
                //这个 graph 表明需要将 verifyPod 迁移到 nodeWithPod

                if (!ScheduleUtils.resourceFillOnePod(nodeWithPod, verifyPod)) {
                    return Collections.emptyList();
                }

                if (!ScheduleUtils.layoutFillOnePod(allMaxInstancePerNodeLimit, nodeWithPod, verifyPod)) {
                    return Collections.emptyList();
                }

                // 这不是硬性要求
                //PodPreAlloc podPreAlloc = ScheduleUtils.cgroupFillOnePod(nodeWithPod, verifyPod);
                //
                //if (!podPreAlloc.isSatisfy()) {
                //    return Collections.emptyList();
                //}

                if (CollectionUtils.isNotEmpty(graphNode.getCpuIds())) {
                    verifyPod.setCpuIDs(graphNode.getCpuIds());
                }

                nodeWithPod.getPods().add(verifyPod);

            }

            if (OperationType.OFFLINE.equals(graphNode.getType())) {
                // 从 nodeWithPod 中移除 verifyPod
                nodeWithPod.setPods(nodeWithPod.getPods().stream().filter(pod -> !pod.getPodSn().equals(verifyPod.getPodSn())).collect(toList()));
            }

        }

        return nodeWithPods;

    }

}
