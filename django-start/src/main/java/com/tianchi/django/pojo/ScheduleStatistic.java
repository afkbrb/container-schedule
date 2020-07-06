package com.tianchi.django.pojo;

import java.util.List;

import com.tianchi.django.common.pojo.NodeWithPod;
import com.tianchi.django.common.pojo.Rule;
import com.tianchi.django.common.pojo.associate.GroupRuleAssociate;
import com.tianchi.django.common.utils.ScoreUtils;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import static com.tianchi.django.common.enums.Resource.*;
import static com.tianchi.django.common.utils.NodeWithPodUtils.allocation;
import static com.tianchi.django.common.utils.NodeWithPodUtils.podSize;

@Slf4j
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScheduleStatistic {

    private String directory;

    private int nodeCount;

    private int podCount;

    private float gpuAllocation;

    private float cpuAllocation;

    private float ramAllocation;

    private float diskAllocation;

    private int score;

    public static ScheduleStatistic from(@NonNull String directory, @NonNull List<NodeWithPod> nodeWithPods,
                                         @NonNull Rule rule, @NonNull List<GroupRuleAssociate> groupRuleAssociates) {
        return ScheduleStatistic.builder().directory(directory)
            .nodeCount(nodeWithPods.size()).podCount(podSize(nodeWithPods))
            .gpuAllocation(allocation(nodeWithPods, GPU)).cpuAllocation(allocation(nodeWithPods, CPU))
            .ramAllocation(allocation(nodeWithPods, RAM)).diskAllocation(allocation(nodeWithPods, DISK))
            .score(ScoreUtils.scoreNodeWithPods(nodeWithPods, rule, groupRuleAssociates))
            .build();

    }

    public void log(String label) {

        log.info("schedule statistic | dir: {} label: {} | count:【node {}, pod {}】", directory, label, nodeCount, podCount);

        log.info("schedule statistic | dir: {} label: {} | allocation:【gpu {}%, cpu {}%, ram {}%, disk {}%】", directory, label, gpuAllocation, cpuAllocation, ramAllocation, diskAllocation);

        log.info("schedule statistic | dir: {} label: {} | total score: {}", directory, label, score);

    }

}
