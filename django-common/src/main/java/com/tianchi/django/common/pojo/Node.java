package com.tianchi.django.common.pojo;

import java.util.List;

import com.tianchi.django.common.enums.Resource;
import lombok.*;
import org.apache.commons.collections4.ListUtils;

import static com.tianchi.django.common.enums.Resource.*;
import static java.util.stream.Collectors.toList;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Node {

    private String sn;//宿主机唯一标识。

    private String nodeModelName;//宿主机机型

    private int gpu;//资源

    private int cpu;

    private int ram;

    private int disk;

    private int eni;//弹性网卡ENI约束，约束当前宿主机上容器实例数不允许超过eni数量

    private List<Topology> topologies;//宿主机上socket、core、cpu的拓扑关系结构。

    public int value(Resource type) {
        return type == GPU ? gpu : (type == CPU ? cpu : (type == RAM ? ram : disk));
    }

    public Node copy() {
        return Node.builder()
            .sn(sn).nodeModelName(nodeModelName).gpu(gpu).cpu(cpu).ram(ram).disk(disk).eni(eni)
            .topologies(ListUtils.emptyIfNull(topologies).stream().map(Topology::copy).collect(toList())).build();
    }

}
