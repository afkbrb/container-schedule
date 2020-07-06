package com.tianchi.django.common.utils;

import java.util.List;
import java.util.Map;

import com.tianchi.django.common.enums.Resource;
import com.tianchi.django.common.pojo.Node;
import com.tianchi.django.common.pojo.Topology;
import lombok.NonNull;
import org.apache.commons.collections4.ListUtils;

import static java.util.stream.Collectors.toMap;

public class NodeUtils {

    /**
     * 计算一批宿主机某一项资源的总数
     */
    public static int totalResource(List<Node> nodes, @NonNull Resource resource) {
        return ListUtils.emptyIfNull(nodes).parallelStream().mapToInt(node -> node.value(resource)).sum();
    }

    /**
     * 根据node topologie信息映射<cpu,socket>
     */
    public static Map<Integer, Integer> cpuToSocket(Node node) {
        return ListUtils.emptyIfNull(node.getTopologies()).stream().collect(toMap(Topology::getCpu, Topology::getSocket));
    }

    /**
     * 根据node topologie信息映射<cpu,core>
     */
    public static Map<Integer, Integer> cpuToCore(Node node) {
        return ListUtils.emptyIfNull(node.getTopologies()).stream().collect(toMap(Topology::getCpu, Topology::getCore));
    }

}
