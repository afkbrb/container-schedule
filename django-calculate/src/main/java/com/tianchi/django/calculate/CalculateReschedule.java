package com.tianchi.django.calculate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tianchi.django.common.IReschedule;
import com.tianchi.django.common.enums.Resource;
import com.tianchi.django.common.pojo.*;
import com.tianchi.django.common.pojo.associate.GroupRuleAssociate;
import com.tianchi.django.common.pojo.associate.PodPreAlloc;
import com.tianchi.django.common.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import static com.tianchi.django.common.utils.HCollectors.*;
import static com.tianchi.django.common.utils.ScheduleUtils.*;
import static java.util.Collections.reverseOrder;
import static java.util.Comparator.comparingDouble;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * 我们是亚军！
 */
@Slf4j
public class CalculateReschedule implements IReschedule {

    private long start; // 程序启动时间戳

    private int stage = 1;

    public CalculateReschedule(long start) {
        this.start = start;
    }

    /**
     * 找出所有需要迁移的 pod，放入其他不存在需要迁移的 pod 的 node 中。
     * <p>
     * 同一个 stage 中，不能将一个 pod 迁移到一个也在该 stage 发生迁出动作的 node 中，
     * 因为这样存在二义性：可能先迁出，再迁入 pod，也可能相反，
     */
    @Override
    public List<RescheduleResult> reschedule(final List<NodeWithPod> originalNodeWithPods, final Rule rule) {
        Map<Integer, List<RescheduleResult>> resultMap = new HashMap<>();

        // 重复多次，选取最佳的
        // 我也不知道为什么每次运行结果不同 :(
        for (int i = 1; i <= 10; i++) {
            long start1 = System.currentTimeMillis();
            List<RescheduleResult> rescheduleResults = doReschedule(originalNodeWithPods, rule);
            int rescheduleScore = ScoreUtils.scoreReschedule(rescheduleResults, rule, originalNodeWithPods);
            resultMap.put(rescheduleScore, rescheduleResults);
            long duration = System.currentTimeMillis() - start1;
            System.out.printf("[reschedule] counter: %d, score: %d, duration: %d\n", i, rescheduleScore, duration);
        }

        int min = Integer.MAX_VALUE;
        List<RescheduleResult> result = null;
        for (Map.Entry<Integer, List<RescheduleResult>> entry : resultMap.entrySet()) {
            if (entry.getKey() < min) {
                min = entry.getKey();
                result = entry.getValue();
            }
        }

        System.out.println("return from reschedule, rescheduleScore: " + min);
        assert result != null;
        return result;
    }

    public List<RescheduleResult> doReschedule(List<NodeWithPod> originalNodeWithPods, Rule rule) {
        stage = 1;
        Map<String, Integer> allMaxInstancePerNodeLimit = getAllMaxInstancePerNodeLimit(originalNodeWithPods, rule);

        // 先迁移 invalid pods，nodeWithPods 就是迁移后的结果
        List<NodeWithPod> nodeWithPods = originalNodeWithPods.parallelStream().map(NodeWithPod::copy).collect(toList());
        List<RescheduleResult> migrateInvalidPodsResult = migrateInvalidPods(nodeWithPods, allMaxInstancePerNodeLimit, rule);

        // 将 nodeWithPods 排个序，使用率最差的放前面处理
        nodeWithPods = sortNodeWithPod(nodeWithPods);
        // 现在 nodeWithPods 已经被调整成 valid 的了，我们在此基础上进行优化
        List<RescheduleResult> migrateValidPodsResult = migrateValidPods(nodeWithPods, allMaxInstancePerNodeLimit, rule);

        return ListUtils.union(migrateInvalidPodsResult, migrateValidPodsResult);
    }

    private List<NodeWithPod> sortNodeWithPod(List<NodeWithPod> nodeWithPods) {
        return nodeWithPods.stream().sorted(reverseOrder(comparingDouble(CalculateReschedule::getCov))).collect(toList());
    }

    private Map<String, Integer> getAllMaxInstancePerNodeLimit(List<NodeWithPod> originalNodeWithPods, Rule rule) {
        List<GroupRuleAssociate> groupRuleAssociates = GroupRuleAssociate.fromPods(NodeWithPodUtils.toPods(originalNodeWithPods));
        return RuleUtils.toAllMaxInstancePerNodeLimit(rule, groupRuleAssociates);
    }

    private List<RescheduleResult> migrateValidPods(List<NodeWithPod> nodeWithPods, Map<String, Integer> allMaxInstancePerNodeLimit, Rule rule) {
        // 从头开始，对于每个 node，先将其 pods 中使得离散系数大于一定阈值的移到后面去，
        // 然后从每次从后面所有 pods 中选取一个 best fit 放入当前 node，直到放不下为止
        List<RescheduleResult> migrateValidPodsResult = new ArrayList<>();
        int size = nodeWithPods.size();
        for (int i = 0; i < size; i++) {
            // System.out.printf("working on: %3d\n", i);
            NodeWithPod source = nodeWithPods.get(i);
            List<Pod> podsToMigrate = getPodsToMigrate(source, i, size);
            // 将需要 migrate 的 pods 放到后面
            for (Pod pod : podsToMigrate) {
                for (int j = i + 1; j < size; j++) {
                    NodeWithPod target = nodeWithPods.get(j);
                    List<Integer> availableCpus = getAvailableCpus(pod, target, allMaxInstancePerNodeLimit);
                    if (!availableCpus.isEmpty()) {
                        migrateValidPodsResult.add(migratePod(pod, source, target, availableCpus));
                        break;
                    }
                }
            }

            // 现在 source 有些空位了，从后面选取最佳的放进来，直到放不下为止
            Set<Pair<NodeWithPod, Pod>> toAllocatePods = new HashSet<>();
            for (int j = i + 1; j < size; j++) {
                NodeWithPod target = nodeWithPods.get(j);
                List<Pod> pods = target.getPods();
                for (Pod pod : pods) {
                    toAllocatePods.add(Pair.of(target, pod));
                }
            }

            Node sourceNode = source.getNode();
            List<Pod> pods = source.getPods();
            int[] R = {sourceNode.getCpu(), sourceNode.getRam(), sourceNode.getDisk()};
            int[] C = {PodUtils.totalResource(pods, Resource.CPU), PodUtils.totalResource(pods, Resource.RAM), PodUtils.totalResource(pods, Resource.DISK)};
            Set<Pair<NodeWithPod, Pod>> allocatablePods = getAllocatablePods(toAllocatePods, source, allMaxInstancePerNodeLimit);
            while (!allocatablePods.isEmpty()) {
                Pair<NodeWithPod, Pod> bestFit = getBestFit(R, C, allocatablePods);
                Pod pod = bestFit.getRight();
                List<Integer> availableCpus = getAvailableCpus(pod, source, allMaxInstancePerNodeLimit);
                assert !availableCpus.isEmpty();
                migrateValidPodsResult.add(migratePod(pod, bestFit.getLeft(), source, availableCpus));
                C[0] += pod.getCpu();
                C[1] += pod.getRam();
                C[2] += pod.getDisk();

                allocatablePods.remove(bestFit);
                allocatablePods = getAllocatablePods(allocatablePods, source, allMaxInstancePerNodeLimit);

            }
        }

        migrateValidPodsResult.addAll(compactTail(nodeWithPods, allMaxInstancePerNodeLimit, rule));

        return migrateValidPodsResult;
    }

    private List<Pod> getPodsToMigrate(NodeWithPod nodeWithPod, int index, int size) {
        List<Pod> podsToMigrate = new ArrayList<>();
        Node node = nodeWithPod.getNode();
        for (Pod pod : nodeWithPod.getPods()) {
            double cov = getCov(node, pod);
            double threshold = getThreshold(index, size);
            if (cov > threshold) {
                podsToMigrate.add(pod);
            }
        }
        if (podsToMigrate.size() > 1) {
            // int before = podsToMigrate.size();
            podsToMigrate.remove(podsToMigrate.size() - 1);
            // System.out.printf("podsToMigrate size: %d, return podsToMigrate size: %d\n", before, podsToMigrate.size());
            return podsToMigrate;
        } else {
            // System.out.printf("podsToMigrate size: %d, return empty list\n", podsToMigrate.size());
            return Collections.emptyList();
        }
    }

    /**
     * threshold 可以动态变化
     * 一开始可选的 pod 比较多，可以大一些，因为大概率存在一些较小 pod 的来填充碎片
     * 后面就可以小一些了
     */
    private double getThreshold(int index, int size) {
        return 0.2;
    }


    private Set<Pair<NodeWithPod, Pod>> getAllocatablePods(Set<Pair<NodeWithPod, Pod>> toAllocatePods, NodeWithPod nodeWithPod, Map<String, Integer> allMaxInstancePerNodeLimit) {
        // 当这个 pod 比较不适合其当前所属的 node 时才进行迁移，以减少迁移次数
        return toAllocatePods.stream().filter(pair -> getCov(pair.getLeft().getNode(), pair.getRight()) > 0.2 && !getAvailableCpus(pair.getRight(), nodeWithPod, allMaxInstancePerNodeLimit).isEmpty()).collect(toSet());
    }

    private Pair<NodeWithPod, Pod> getBestFit(int[] R, int[] C, Set<Pair<NodeWithPod, Pod>> allocatablePods) {
        double minCov = Double.MAX_VALUE;
        Pair<NodeWithPod, Pod> bestFit = null;
        // 找出使得离散系数最小的那个
        for (Pair<NodeWithPod, Pod> pair : allocatablePods) {
            double cov = getCov(R, C, pair.getRight());
            if (cov < minCov) {
                minCov = cov;
                bestFit = pair;
            }
        }
        return bestFit;
    }

    private static double getCov(NodeWithPod nodeWithPods) {
        int CPU = nodeWithPods.getNode().getCpu();
        int RAM = nodeWithPods.getNode().getRam();
        int totalCPU = PodUtils.totalResource(nodeWithPods.getPods(), Resource.CPU);
        int totalRam = PodUtils.totalResource(nodeWithPods.getPods(), Resource.CPU);
        if (totalCPU == 0 || totalRam == 0) return 0.0;
        double x = (double) totalCPU / CPU;
        double y = (double) totalRam / RAM;
        return Math.abs(x - y) / (x + y);
    }

    /**
     * x = cpu / R[0], y = ram / R[1], avg = (x + y) / 2
     * cov = sqrt(((x - avg)^2 + (y - avg)^2) / 2) / avg
     * = |x - y| / (x + y)
     */
    private static double getCov(Node node, Pod pod) {
        double x = (double) (pod.getCpu()) / node.getCpu();
        double y = (double) (pod.getRam()) / node.getRam();
        double cov = Math.abs(x - y) / (x + y);
        // System.out.printf("cov = %.4f, x = %.4f = %3d/%3d, y = %.4f = %3d/%3d\n", cov, x, pod.getCpu(), R[0], y, pod.getRam(), R[1]);
        return cov;
    }


    private static double getCov(int[] R, int[] C, Pod pod) {
        double x = (double) (C[0] + pod.getCpu()) / R[0];
        double y = (double) (C[1] + pod.getRam()) / R[1];
        return Math.abs(x - y) / (x + y);
    }

    /**
     * 将尾部的 node 中的 pods 尽可能地移到前面去
     */
    private List<RescheduleResult> compactTail(List<NodeWithPod> nodeWithPods, Map<String, Integer> allMaxInstancePerNodeLimit, Rule rule) {
        List<RescheduleResult> migrateValidPodsResult = new ArrayList<>();
        for (int i = nodeWithPods.size() - 1; i >= 0; i--) {
            NodeWithPod source = nodeWithPods.get(i);
            List<Pod> pods = new ArrayList<>(source.getPods());
            for (Pod pod : pods) {
                for (int j = 0; j < i; j++) {
                    NodeWithPod target = nodeWithPods.get(j);
                    List<Integer> availableCpus = getAvailableCpus(pod, target, allMaxInstancePerNodeLimit);
                    if (!availableCpus.isEmpty()) {
                        migrateValidPodsResult.add(migratePod(pod, source, target, availableCpus));
                        break;
                    }
                }
            }
            if (!source.getPods().isEmpty()) break;
            // System.out.println("release " + i);
        }
        return migrateValidPodsResult;
    }

    /**
     * 先使得这批 nodeWithPods 变合法。
     */
    private List<RescheduleResult> migrateInvalidPods(List<NodeWithPod> nodeWithPods, Map<String, Integer> allMaxInstancePerNodeLimit, Rule rule) {
        // 找出 invalid 的 pods
        // 已经从 nodeWithPods 移除了 invalid pods
        Map<String, List<Pod>> nodeToValidPods = getInvalidPods(nodeWithPods, rule);

        List<RescheduleResult> rescheduleResults = Lists.newArrayList();

        for (Map.Entry<String, List<Pod>> entry : nodeToValidPods.entrySet()) {
            String sourceSn = entry.getKey();

            // 将违背规则的 pod 迁移到其他容器
            for (Pod pod : entry.getValue()) {
                for (NodeWithPod nodeWithPod : nodeWithPods) {
                    if (sourceSn.equals(nodeWithPod.getNode().getSn())) continue;

                    // 看看这个 pod 是否能放入当前 node
                    // staticFillOnePod 成功的话，pod 就已经被放入了 nodeWithPod 中
                    if (ScheduleUtils.staticFillOnePod(nodeWithPod, pod, allMaxInstancePerNodeLimit)) {
                        rescheduleResults.add(RescheduleResult.builder()
                                .stage(stage++).sourceSn(sourceSn).targetSn(nodeWithPod.getNode().getSn())
                                .podSn(pod.getPodSn()).cpuIDs(pod.getCpuIDs())
                                .build()
                        );
                        break;
                    }
                }

            }
        }

        // log.info("after migrate, nodeWithPods size: {}", NodeWithPodUtils.toPods(nodeWithPods).size());

        return rescheduleResults;
    }

    private List<Integer> getAvailableCpus(Pod pod, NodeWithPod nodeWithPod, Map<String, Integer> allMaxInstancePerNodeLimit) {
        if (!(resourceFillOnePod(nodeWithPod, pod) && layoutFillOnePod(allMaxInstancePerNodeLimit, nodeWithPod, pod))) {
            return Collections.emptyList();
        }

        PodPreAlloc podPreAlloc = cgroupFillOnePod(nodeWithPod, pod);
        return podPreAlloc.isSatisfy() && !podPreAlloc.getCpus().isEmpty() ? podPreAlloc.getCpus() : Collections.emptyList();
    }

    /**
     * 找出这个集群中违背规则的所有的容器列表(贪心)。
     * <p>
     * 返回 node -> invalid pods in the node
     */
    private Map<String, List<Pod>> getInvalidPods(List<NodeWithPod> nodeWithPods, Rule rule) {

        Map<String, List<Pod>> result = Maps.newHashMap();

        // 先过滤不满足资源分配的容器，nodeWithPods 数据会被修改
        searchResourceAgainstPods(nodeWithPods)
                .forEach((k, v) -> result.compute(k, (key, old) -> old == null ? v : ListUtils.union(v, old)));

        // 再过滤不满足布局的容器，nodeWithPods 数据会被修改
        searchLayoutAgainstPods(nodeWithPods, rule)
                .forEach((k, v) -> result.compute(k, (key, old) -> old == null ? v : ListUtils.union(v, old)));

        // 再过滤不满足 cpu 分配的容器，nodeWithPods 数据会被修改
        searchCgroupAgainstPods(nodeWithPods)
                .forEach((k, v) -> result.compute(k, (key, old) -> old == null ? v : ListUtils.union(v, old)));

        return result;

    }

    private RescheduleResult migratePod(Pod pod, NodeWithPod source, NodeWithPod target, List<Integer> cpus) {
        source.getPods().remove(pod);
        pod.setCpuIDs(cpus);
        target.getPods().add(pod);
        return RescheduleResult.builder()
                .stage(stage++).sourceSn(source.getNode().getSn()).targetSn(target.getNode().getSn())
                .podSn(pod.getPodSn()).cpuIDs(pod.getCpuIDs())
                .build();
    }

    /**
     * 建立 sn -> 违背资源规则的 pod list 映射。
     * 没有违背的会继续保留在 NodeWithPod 中。
     */
    private Map<String, List<Pod>> searchResourceAgainstPods(List<NodeWithPod> nodeWithPods) {
        return nodeWithPods.parallelStream()
                .map(nwp -> {
                    List<Pod> againstPods = Lists.newArrayList(), tmpPods = Lists.newArrayList(), normalPods = Lists.newArrayList();
                    //校验资源不满足的容器
                    for (Pod pod : nwp.getPods()) {
                        boolean against = false;

                        for (Resource resource : Resource.class.getEnumConstants()) {
                            int nodeResource = nwp.getNode().value(resource);
                            int podsResource = PodUtils.totalResource(ListUtils.union(tmpPods, ImmutableList.of(pod)), resource);

                            if (nodeResource < podsResource) {
                                againstPods.add(pod);
                                against = true;
                                break; // 违背任意一个资源限制就抬走
                            }
                        }

                        if (!against) {
                            tmpPods.add(pod); // 不违背的继续加入 tmpPods
                        }
                    }

                    int eniAgainstPodSize = tmpPods.size() - nwp.getNode().getEni();

                    // 校验超过 eni 约束的容器
                    if (eniAgainstPodSize > 0) {
                        againstPods.addAll(tmpPods.subList(0, eniAgainstPodSize));
                        normalPods.addAll(tmpPods.subList(eniAgainstPodSize, tmpPods.size()));
                    } else {
                        normalPods.addAll(tmpPods);
                    }

                    nwp.setPods(normalPods); // 贪心判断的正常容器继续放在该机器中
                    return ImmutablePair.of(nwp.getNode().getSn(), againstPods);
                })
                .filter(pair -> CollectionUtils.isNotEmpty(pair.getRight())).collect(pairToMap());
    }

    // 违背布局规则容器准备重新调度。node_sn -> List<Pod>>
    private Map<String, List<Pod>> searchLayoutAgainstPods(List<NodeWithPod> nodeWithPods, Rule rule) {
        List<GroupRuleAssociate> groupRuleAssociates = GroupRuleAssociate.fromPods(NodeWithPodUtils.toPods(nodeWithPods));
        Map<String, Integer> maxInstancePerNodes = RuleUtils.toAllMaxInstancePerNodeLimit(rule, groupRuleAssociates);
        return nodeWithPods.parallelStream()
                .map(nwp -> {
                    Map<String, Integer> groupCountPreNodeMap = Maps.newHashMap();
                    List<Pod> againstPods = Lists.newArrayList(), normalPods = Lists.newArrayList();

                    for (Pod pod : nwp.getPods()) {
                        int maxInstancePerNode = maxInstancePerNodes.get(pod.getGroup());
                        int oldValue = groupCountPreNodeMap.getOrDefault(pod.getGroup(), 0);
                        if (oldValue == maxInstancePerNode) {
                            againstPods.add(pod);
                            continue;
                        }
                        groupCountPreNodeMap.put(pod.getGroup(), oldValue + 1);
                        normalPods.add(pod);
                    }

                    nwp.setPods(normalPods); // 贪心判断的正常容器继续放在该机器中
                    return ImmutablePair.of(nwp.getNode().getSn(), againstPods);
                })
                .filter(pair -> CollectionUtils.isNotEmpty(pair.getRight())).collect(pairToMap());
    }

    // 违背 cpu 绑核分配规则容器准备重新调度。<node_sn,List<Pod>>
    private Map<String, List<Pod>> searchCgroupAgainstPods(List<NodeWithPod> nodeWithPods) {
        return nodeWithPods.parallelStream()
                .map(nwp -> {
                    Node node = nwp.getNode();
                    // node 中不存在 topologies, 不校验绑核。
                    if (CollectionUtils.isEmpty(node.getTopologies())) {
                        return ImmutablePair.<String, List<Pod>>nullPair();
                    }

                    // node 中不存在 pods, 不校验绑核。
                    if (CollectionUtils.isEmpty(nwp.getPods())) {
                        return ImmutablePair.<String, List<Pod>>nullPair();
                    }

                    List<Pod> againstPods = Lists.newArrayList();
                    // 这台机器上重叠的cpuId分配
                    Set<Integer> againstCpuIds = NodeWithPodUtils.cpuIDCountMap(nwp).entrySet().stream()
                            .filter(entry -> entry.getValue() > 1).map(Map.Entry::getKey).collect(toSet());
                    Map<Integer, Integer> cpuToSocket = NodeUtils.cpuToSocket(node);
                    Map<Integer, Integer> cpuToCore = NodeUtils.cpuToCore(node);
                    List<Pod> normalPods = Lists.newArrayList();
                    for (Pod pod : nwp.getPods()) {
                        if (CollectionUtils.isEmpty(pod.getCpuIDs())) { // 没有分配 cpuId 的容器
                            againstPods.add(pod);
                            continue;
                        }

                        // 贪心选择包含重叠 cpu 的容器
                        Set<Integer> intersectionCpuIds = Sets.intersection(againstCpuIds, Sets.newHashSet(pod.getCpuIDs()));
                        if (CollectionUtils.isNotEmpty(intersectionCpuIds)) {
                            againstCpuIds.removeAll(pod.getCpuIDs());
                            againstPods.add(pod);
                            continue;
                        }

                        // 跨 socket 容器
                        long socketCount = pod.getCpuIDs().stream().map(cpuToSocket::get).distinct().count();
                        if (socketCount > 1) {
                            againstPods.add(pod);
                            continue;
                        }

                        // 同 core
                        Map<Integer, Integer> sameCoreMap = pod.getCpuIDs().stream().collect(countingInteger(cpuToCore::get))
                                .entrySet().stream().filter(entry -> entry.getValue() > 1).collect(entriesToMap());
                        if (MapUtils.isNotEmpty(sameCoreMap)) {
                            againstPods.add(pod);
                            continue;
                        }

                        //TODO 已经很复杂了，如果排名拉不开差距再增加 sensitiveCpuBind 数据校验

                        normalPods.add(pod);
                    }

                    nwp.setPods(normalPods); // 贪心判断的正常容器继续放在该机器中
                    return ImmutablePair.of(nwp.getNode().getSn(), againstPods);

                })
                .filter(pair -> !pair.equals(ImmutablePair.<String, List<Pod>>nullPair()))
                .filter(pair -> CollectionUtils.isNotEmpty(pair.getRight())).collect(pairToMap());
    }

}
