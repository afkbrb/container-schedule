package com.tianchi.django.common.pojo;

import java.util.List;

import com.tianchi.django.common.pojo.rule.*;
import lombok.*;
import org.apache.commons.collections4.ListUtils;

import static java.util.stream.Collectors.toList;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Rule {

    private int timeLimitInMins;//约束程序执行的最长时间

    private int defaultMaxInstancePerNode;//默认的单机单应用最大堆叠容器数量。单机应用堆叠若无其他指定，依次数量为约束

    private ScoreWeight scoreWeight;//打分的权重数据，避免硬编码

    private List<ReplicasMaxInstancePerNode> replicasMaxInstancePerNodes;//根据需要应用需要扩容的实例数，约束单机单应用最大堆叠数。

    private List<ResourceWeight> nodeResourceWeights;//宿主机资源权重数据

    public Rule copy() {
        return Rule.builder()
            .timeLimitInMins(timeLimitInMins).defaultMaxInstancePerNode(defaultMaxInstancePerNode).scoreWeight(scoreWeight.copy())
            .replicasMaxInstancePerNodes(ListUtils.emptyIfNull(replicasMaxInstancePerNodes).stream().map(ReplicasMaxInstancePerNode::copy).collect(toList()))
            .nodeResourceWeights(ListUtils.emptyIfNull(nodeResourceWeights).stream().map(ResourceWeight::copy).collect(toList())).build();
    }

}
