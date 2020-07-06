package com.tianchi.django.common.utils;

import java.util.*;

import com.google.common.collect.Lists;
import com.tianchi.django.common.pojo.associate.PodPreAlloc;
import com.tianchi.django.common.pojo.*;
import org.apache.commons.collections4.CollectionUtils;

import static com.tianchi.django.common.constants.DjangoConstants.MILLISECONDS_4_ONE_MIN;
import static com.tianchi.django.common.enums.Resource.*;
import static java.util.stream.Collectors.*;

public class ScheduleUtils {

    /**
     * 是否满足静态布局
     */
    public static boolean staticFillOnePod(NodeWithPod nwp, Pod pod, Map<String, Integer> allMaxInstancePerNodeLimit) {

        List<Pod> pods = nwp.getPods();

        if (!resourceFillOnePod(nwp, pod)) {
            return false;
        }

        if (!layoutFillOnePod(allMaxInstancePerNodeLimit, nwp, pod)) {
            return false;
        }

        PodPreAlloc podPreAlloc = cgroupFillOnePod(nwp, pod);

        if (!podPreAlloc.isSatisfy()) {
            return false;
        }

        if (CollectionUtils.isNotEmpty(podPreAlloc.getCpus())) {
            pod.setCpuIDs(podPreAlloc.getCpus());
        }

        pods.add(pod);

        return true;

    }

    /**
     * 资源校验,如果当前宿主机能够容纳下app构建的一个pod，则返回true
     */
    public static boolean resourceFillOnePod(NodeWithPod nwp, Pod pod) {

        Node node = nwp.getNode();

        List<Pod> pods = nwp.getPods();

        if (NodeWithPodUtils.surplusResource(nwp, GPU) < pod.getGpu()) {//gpu资源约束
            return false;
        }

        if (NodeWithPodUtils.surplusResource(nwp, CPU) < pod.getCpu()) {//cpu资源约束
            return false;
        }

        if (NodeWithPodUtils.surplusResource(nwp, RAM) < pod.getRam()) {//内存资源约束
            return false;
        }

        if (NodeWithPodUtils.surplusResource(nwp, DISK) < pod.getDisk()) {//磁盘资源约束
            return false;
        }

        return node.getEni() - pods.size() >= 1;//eni资源约束,当前pod是否还能放入此node

    }

    /**
     * 布局上的堆叠约束。校验是否超过堆叠上限
     */
    public static boolean layoutFillOnePod(Map<String, Integer> allMaxInstancePerNodeLimit, NodeWithPod nwp, Pod verifyPod) {//MaxInstancePerNode约束

        int maxInstancePerNodeLimit = allMaxInstancePerNodeLimit.get(verifyPod.getGroup());

        return nwp.getPods().stream().filter(pod -> pod.getGroup().equals(verifyPod.getGroup())).count() + 1 <= maxInstancePerNodeLimit;

    }

    /**
     * 静态布局上的CPU调度
     */
    public static PodPreAlloc cgroupFillOnePod(NodeWithPod nwp, Pod pod) {

        Node node = nwp.getNode();

        if (CollectionUtils.isEmpty(node.getTopologies())) {
            return PodPreAlloc.EMPTY_SATISFY;
        }

        List<Pod> pods = nwp.getPods();

        //当前宿主机已经使用的cpu
        Set<Integer> usedCpuSet = pods.stream().map(Pod::getCpuIDs).flatMap(Collection::stream).collect(toSet());

        //剩余可用的cpu数量
        List<Topology> useableTopologies = node.getTopologies().stream().filter(topology -> !usedCpuSet.contains(topology.getCpu())).collect(toList());

        //简单快速判断:当前宿主机剩余cpu已经不足以分配给该容器
        if (useableTopologies.size() < pod.getCpu()) {
            return PodPreAlloc.EMPTY_NOT_SATISFY;
        }

        //按照socket划分topologys，<socket,topologys>
        Map<Integer, List<Topology>> socketMap = useableTopologies.stream().collect(groupingBy(Topology::getSocket));

        for (Map.Entry<Integer, List<Topology>> socketEntry : socketMap.entrySet()) {

            List<Topology> socketTopologys = socketEntry.getValue();

            //当前socket下cpu不足以满足容器分配，规避跨socket绑定cpu问题
            if (socketTopologys.size() < pod.getCpu()) {
                continue;
            }

            //同一socket下按照core划分topologys，<core,topologys>
            Map<Integer, List<Topology>> coreMap = socketTopologys.stream().collect(groupingBy(Topology::getCore));

            //当前socket下不同core绑定cpu资源不足以满足容器分配，规避同一容器同core分配cpu问题。
            // 不同 core 的个数小于所需的 cpu 数
            if (coreMap.size() < pod.getCpu()) {
                continue;
            }

            List<Integer> cpus = Lists.newArrayList();

            for (Map.Entry<Integer, List<Topology>> coreEntry : coreMap.entrySet()) {

                List<Topology> coreTopologys = coreEntry.getValue();

                cpus.add(coreTopologys.get(0).getCpu());

                if (cpus.size() == pod.getCpu()) {
                    break;
                }

            }

            //为此容器分配cpu
            return PodPreAlloc.builder().satisfy(true).cpus(cpus).build();

        }

        return PodPreAlloc.EMPTY_NOT_SATISFY;
    }

    /**
     * 校验是否超过规定执行时间上限
     */
    public static boolean ruleOverrunTimeLimit(Rule rule, long start) {
        return System.currentTimeMillis() - start > rule.getTimeLimitInMins() * MILLISECONDS_4_ONE_MIN;
    }
}
