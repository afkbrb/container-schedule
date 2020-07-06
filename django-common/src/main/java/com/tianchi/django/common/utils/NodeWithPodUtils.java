package com.tianchi.django.common.utils;

import java.util.*;

import com.tianchi.django.common.enums.Resource;
import com.tianchi.django.common.pojo.*;
import lombok.NonNull;
import org.apache.commons.collections4.ListUtils;

import static com.tianchi.django.common.utils.HCollectors.*;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class NodeWithPodUtils {

    /**
     * 当前nwp列表中所有的容器数量
     */
    public static int podSize(List<NodeWithPod> nwps) {
        return ListUtils.emptyIfNull(nwps).parallelStream().mapToInt(nwp -> nwp.getPods().size()).sum();
    }

    /**
     * 当前nwp列表中所有的pod列表
     */
    public static List<Pod> toPods(List<NodeWithPod> nwps) {
        return ListUtils.emptyIfNull(nwps).parallelStream().map(NodeWithPod::getPods).flatMap(Collection::stream).collect(toList());
    }

    public static Map<String, Pod> toPodMap(List<NodeWithPod> nwps) {
        return toPods(nwps).stream().collect(HCollectors.objectToKeyMap(Pod::getPodSn));
    }

    /**
     * 当前nwp列表中所有的node列表
     */
    public static List<Node> toNodes(List<NodeWithPod> nwps) {
        return ListUtils.emptyIfNull(nwps).parallelStream().map(NodeWithPod::getNode).collect(toList());
    }

    public static Map<String, Node> toNodeMap(List<NodeWithPod> nwps) {
        return toNodes(nwps).stream().collect(HCollectors.objectToKeyMap(Node::getSn));
    }

    /**
     * 当前nwp剩余resource数量
     */
    public static int surplusResource(NodeWithPod nwp, @NonNull Resource resource) {
        return nwp.getNode().value(resource) - PodUtils.totalResource(nwp.getPods(), resource);
    }

    /**
     * 当前nwp列表在resource上的分配率
     */
    public static float allocation(List<NodeWithPod> nwps, @NonNull Resource resource) {
        return 100f * PodUtils.totalResource(toPods(nwps), resource) / NodeUtils.totalResource(toNodes(nwps), resource);
    }

    /**
     * 当前nwp上分组对应的数量
     */
    public static Map<String, Integer> groupCountPreNodeMap(@NonNull NodeWithPod nwp) {
        return nwp.getPods().stream().collect(countingInteger(Pod::getGroup));
    }

    /**
     * 当前nwp中的容器所被分配的cpuId及对应数量
     */
    public static Map<Integer, Integer> cpuIDCountMap(@NonNull NodeWithPod nwp) {
        return ListUtils.emptyIfNull(nwp.getPods()).stream().map(Pod::getCpuIDs).flatMap(Collection::stream).collect(countingInteger())
            .entrySet().stream().collect(entriesToMap());
    }

    /**
     * 通过原始的nodes、apps信息结合静态布局结果数据转化成为一个集群的静态布局信息。
     */
    public static List<NodeWithPod> resultToNodeWithPods(List<Node> nodes, List<App> apps, List<ScheduleResult> results) {

        Map<String, Node> nodeMap = ListUtils.emptyIfNull(nodes).stream().collect(objectToKeyMap(Node::getSn));

        Map<String, App> appMap = ListUtils.emptyIfNull(apps).stream().collect(objectToKeyMap(App::getGroup));

        return ListUtils.emptyIfNull(results).stream().collect(groupingBy(ScheduleResult::getSn)).entrySet().parallelStream()
            .map(
                entry -> {

                    Node node = nodeMap.get(entry.getKey());

                    List<Pod> pods = ListUtils.emptyIfNull(entry.getValue()).stream()
                        .map(
                            sr -> {
                                Pod pod = PodUtils.toPod(appMap.get(sr.getGroup()));
                                pod.setCpuIDs(sr.getCpuIDs());
                                return pod;
                            }
                        )
                        .collect(toList());

                    return NodeWithPod.builder().node(node).pods(pods).build();
                }
            )
            .collect(toList());

    }

}
