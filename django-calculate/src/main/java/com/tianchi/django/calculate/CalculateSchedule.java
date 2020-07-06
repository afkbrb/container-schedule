package com.tianchi.django.calculate;

import com.google.common.collect.Lists;
import com.tianchi.django.common.ISchedule;
import com.tianchi.django.common.pojo.*;
import com.tianchi.django.common.pojo.associate.GroupRuleAssociate;
import com.tianchi.django.common.pojo.associate.PodPreAlloc;
import com.tianchi.django.common.utils.NodeWithPodUtils;
import com.tianchi.django.common.utils.PodUtils;
import com.tianchi.django.common.utils.RuleUtils;
import com.tianchi.django.common.utils.ScoreUtils;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

import static com.tianchi.django.common.utils.ScheduleUtils.*;
import static java.util.Collections.reverseOrder;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.*;

/**
 * 我们是亚军！
 */
@Slf4j
@AllArgsConstructor
public class CalculateSchedule implements ISchedule {

    private long start; // 对象构建时传入的程序启动时间戳

    @Override
    public List<ScheduleResult> schedule(@NonNull final List<Node> nodes, @NonNull final List<App> apps, @NonNull final Rule rule) {
        Map<Integer, List<ScheduleResult>> resultMap = new HashMap<>();

        // 多试几次，选最高分的
        // 我也不知道为什么每次运行结果不同 :(
        for (int i = 1; i <= 3; i++) {
            long start1 = System.currentTimeMillis();
            List<ScheduleResult> scheduleResults = doSchedule(nodes, apps, rule);
            List<NodeWithPod> nodeWithPods = NodeWithPodUtils.resultToNodeWithPods(nodes, apps, scheduleResults);
            int scheduleScore = ScoreUtils.scoreNodeWithPods(nodeWithPods, rule, GroupRuleAssociate.fromApps(apps));
            resultMap.put(scheduleScore, scheduleResults);
            long duration = System.currentTimeMillis() - start1;
            System.out.printf("[schedule] counter: %d, score: %d, duration: %d\n", i, scheduleScore, duration);
        }

        int min = Integer.MAX_VALUE;
        List<ScheduleResult> result = null;
        for (Map.Entry<Integer, List<ScheduleResult>> entry : resultMap.entrySet()) {
            if (entry.getKey() < min) {
                min = entry.getKey();
                result = entry.getValue();
            }
        }

        System.out.println("return from schedule, scheduleScore: " + min);
        assert result != null;
        return result;
    }

    public List<ScheduleResult> doSchedule(List<Node> nodes, List<App> apps, Rule rule) {
        Map<String, Integer> allMaxInstancePerNodeLimit = RuleUtils.toAllMaxInstancePerNodeLimit(rule, GroupRuleAssociate.fromApps(apps));

        List<NodeWithPod> nodeWithPods = sortAndInitNodeWithPods(nodes, rule);

        Set<Pod> toAllocatePods = new HashSet<>();
        apps.forEach(app -> {
            for (int i = 0; i < app.getReplicas(); i++) {
                toAllocatePods.add(PodUtils.toPod(app));
            }
        });

        // 有点模仿 https://people.eecs.berkeley.edu/~alig/papers/drf.pdf
        for (NodeWithPod nodeWithPod : nodeWithPods) {
            Node node = nodeWithPod.getNode();
            int[] R = {node.getCpu(), node.getRam(), node.getDisk()}; // total resource capacities
            int[] C = {0, 0, 0}; // consumed resources, initially 0

            // 过滤能够放入该 node 的 pod，只要还有能放入该 node 的 pod，就继续循环
            Set<Pod> allocatablePods = getAllocatablePods(toAllocatePods, nodeWithPod, allMaxInstancePerNodeLimit);
            // 第一次先插入出现频率高的 group 中的 pod，
            // 否则这些 pod 将被留到尾部，由于 layout 限制的存在，一个 node 上只能放 1、2 个 pod
            if (!allocatablePods.isEmpty()) {

                List<Pod> firstFits = getFirstFits(R, C, allocatablePods);
                if (firstFits.size() > 0) {
                    Pod firstFit = firstFits.get(0);
                    updateConsumedResource(C, firstFit);

                    PodPreAlloc podPreAlloc = cgroupFillOnePod(nodeWithPod, firstFit);
                    assert podPreAlloc.isSatisfy() && !podPreAlloc.getCpus().isEmpty();
                    firstFit.setCpuIDs(podPreAlloc.getCpus());
                    nodeWithPod.getPods().add(firstFit);
                    toAllocatePods.remove(firstFit);
                    allocatablePods.remove(firstFit);
                    // 下一次 allocatable 可能是这次 allocatable 的子集
                    allocatablePods = getAllocatablePods(allocatablePods, nodeWithPod, allMaxInstancePerNodeLimit);

                    System.out.printf("first fit %s {%d, %d, %d} %s {%d, %d, %d}\n", node.getSn(), node.getCpu(), node.getRam(), node.getDisk(), firstFit.getGroup(), firstFit.getCpu(), firstFit.getRam(), firstFit.getDisk());
                }

                // 注意前面已经移除了一个 pod
                // 再注意需要重新判断这次这个 pod 是否还满足分配条件
                // 因为我们只能保证 firstFits 中放任意一个都是满足条件的，但放多个的话，可能会 GG
                if (firstFits.size() > 1 && canAddPodToNode(firstFits.get(1), nodeWithPod, allMaxInstancePerNodeLimit)) {
                    Pod secondFit = firstFits.get(1);
                    updateConsumedResource(C, secondFit);

                    PodPreAlloc podPreAlloc = cgroupFillOnePod(nodeWithPod, secondFit);
                    assert podPreAlloc.isSatisfy() && !podPreAlloc.getCpus().isEmpty();
                    secondFit.setCpuIDs(podPreAlloc.getCpus());
                    nodeWithPod.getPods().add(secondFit);
                    toAllocatePods.remove(secondFit);
                    allocatablePods.remove(secondFit);
                    // 下一次 allocatable 可能是这次 allocatable 的子集
                    allocatablePods = getAllocatablePods(allocatablePods, nodeWithPod, allMaxInstancePerNodeLimit);

                    System.out.printf("second fit %s {%d, %d, %d} %s {%d, %d, %d}\n", node.getSn(), node.getCpu(), node.getRam(), node.getDisk(), secondFit.getGroup(), secondFit.getCpu(), secondFit.getRam(), secondFit.getDisk());
                }
            }

            while (!allocatablePods.isEmpty()) {

                Pod bestFit = getBestFit(R, C, allocatablePods);
                assert bestFit != null;

                System.out.printf("left: %d, %s {%d, %d, %d}, %s {%d, %d, %d}, CPU: %.2f%%=%d/%d, RAM: %.2f%%=%d/%d, DISK: %.2f%%=%d/%d\n", toAllocatePods.size() - 1, node.getSn(), R[0], R[1], R[2], bestFit.getGroup(), bestFit.getCpu(), bestFit.getRam(), bestFit.getDisk(), 100.0 * C[0] / R[0], C[0], R[0], 100.0 * C[1] / R[1], C[1], R[1], 100.0 * C[2] / R[2], C[2], R[2]);

                updateConsumedResource(C, bestFit);

                PodPreAlloc podPreAlloc = cgroupFillOnePod(nodeWithPod, bestFit);
                assert podPreAlloc.isSatisfy() && !podPreAlloc.getCpus().isEmpty();
                bestFit.setCpuIDs(podPreAlloc.getCpus());
                nodeWithPod.getPods().add(bestFit);
                toAllocatePods.remove(bestFit);
                allocatablePods.remove(bestFit);
                // 下一次 allocatable 可能是这次 allocatable 的子集
                allocatablePods = getAllocatablePods(allocatablePods, nodeWithPod, allMaxInstancePerNodeLimit);
            }

            System.out.printf("finished: %s {%d, %d, %d}, CPU: %.2f%%=%d/%d, RAM: %.2f%%=%d/%d, DISK: %.2f%%=%d/%d\n\n", node.getSn(), R[0], R[1], R[2], 100.0 * C[0] / R[0], C[0], R[0], 100.0 * C[1] / R[1], C[1], R[1], 100.0 * C[2] / R[2], C[2], R[2]);
        }

        // 将每个 nodeWithPod 中的 pod 信息提取出来组合成 ScheduleResult。
        return nodeWithPods.parallelStream()
                .map(nwp -> nwp.getPods().stream()
                        .map(pod -> ScheduleResult.builder().sn(nwp.getNode().getSn()).group(pod.getGroup()).cpuIDs(pod.getCpuIDs()).build())
                        .collect(toList()))
                .flatMap(Collection::stream).collect(toList());
    }

    private List<Pod> getFirstFits(int[] R, int[] C, Set<Pod> pods) {
        List<List<Pod>> sortedGroup = pods.stream()
                .collect(groupingBy(Pod::getGroup))
                .values().stream()
                .sorted(reverseOrder(comparing(List::size)))
                .collect(toList());

        if (sortedGroup.size() > 1 && sortedGroup.get(1).size() > 100) { // 剩余比较多时才返回两个
            System.out.printf("first size: %d, second size: %d\n", sortedGroup.get(0).size(), sortedGroup.get(1).size());
            return Arrays.asList(sortedGroup.get(0).get(0), sortedGroup.get(1).get(0));
        } else if (sortedGroup.size() > 0) {
            // System.out.printf("first size: %d\n", sortedGroup.get(0).size());
            return Collections.singletonList(sortedGroup.get(0).get(0));
        } else {
            // System.out.println("getFirstFits returns empty list");
            return Collections.emptyList();
        }
    }

    private Pod getBestFit(int[] R, int[] C, Set<Pod> pods) {
        double minCov = Double.MAX_VALUE;
        Pod bestFit = null;
        // 找出使得离散系数最小的那个
        for (Pod pod : pods) {
            double cov = getCov(R, C, pod);
            if (cov < minCov) {
                minCov = cov;
                bestFit = pod;
            }
        }
        return bestFit;
    }

    /**
     * 使用离散程度而不是方差是为了优先选择比较大的 pod 插入。
     * 比如 {1, 2, 3} 的方差和 {4, 5, 6} 的相同，但后者的离散程度更小。
     *
     *
     * x = cpu / R[0], y = ram / R[1], avg = (x + y) / 2
     * cov = sqrt(((x - avg)^2 + (y - avg)^2) / 2) / avg
     * = |x - y| / (x + y)
     */
    private double getCov(int[] R, int[] C, Pod pod) {
        // 算 3 个维度
        // double x = (double) (C[0] + pod.getCpu()) / R[0];
        // double y = (double) (C[1] + pod.getRam()) / R[1];
        // double z = (double) (C[2] + pod.getDisk()) / R[2];
        // // 为减少计算，此处计算的是离散系数的平方，也就是 方差/均值^2
        // double avg = (x + y + z) / 3.0;
        // // 不除 3 不影响排序
        // double variance = (x - avg) * (x - avg) + (y - avg) * (y - avg) + (z - avg) * (z - avg);
        // return variance / (avg * avg); // coefficient of variation

        // 发现只算两个效果好些，应该是因为磁盘资源大多数情况下是够用的，可以不考虑，考虑的话反而会干扰
        double x = (double) (C[0] + pod.getCpu()) / R[0];
        double y = (double) (C[1] + pod.getRam()) / R[1];
        return Math.abs(x - y) / (x + y);
    }

    private void updateConsumedResource(int[] C, Pod pod) {
        C[0] += pod.getCpu();
        C[1] += pod.getRam();
        C[2] += pod.getDisk();
    }

    private Set<Pod> getAllocatablePods(Set<Pod> toAllocatePods, NodeWithPod nodeWithPod, Map<String, Integer> allMaxInstancePerNodeLimit) {
        return toAllocatePods.stream().filter(pod -> canAddPodToNode(pod, nodeWithPod, allMaxInstancePerNodeLimit)).collect(toSet());
    }

    private boolean canAddPodToNode(Pod pod, NodeWithPod nodeWithPod, Map<String, Integer> allMaxInstancePerNodeLimit) {
        if (!(resourceFillOnePod(nodeWithPod, pod) && layoutFillOnePod(allMaxInstancePerNodeLimit, nodeWithPod, pod))) {
            return false;
        }

        PodPreAlloc podPreAlloc = cgroupFillOnePod(nodeWithPod, pod);
        return podPreAlloc.isSatisfy() && !podPreAlloc.getCpus().isEmpty();
    }

    /**
     * 计算每个机器 node 的成本，按成本从高到低排序，然后将 node 转化成 NodeWithPod。
     * 通过 node 按照规则分数从大到小排序, 然后 nodes 信息转化为能够容下 pod 对象的 NodeWithPod 对象，与 node 对象一对一构建。
     * <p>
     * 优先使用成本高的机器，因为最后会产生很多碎片，我们需要这些碎片存在于较低成本的机器上
     *
     * @param nodes 宿主机列表
     * @param rule  规则对象
     * @return 能够容下 pod 对象的 NodeWithPod 对象
     */
    private List<NodeWithPod> sortAndInitNodeWithPods(List<Node> nodes, Rule rule) {
        return nodes.parallelStream()
                .sorted(reverseOrder(Comparator.comparingInt(node -> ScoreUtils.resourceScore(node, rule))))
                .map(node -> NodeWithPod.builder().node(node).pods(Lists.newArrayList()).build())
                .collect(toList());
    }

}
