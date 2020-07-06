package com.tianchi.django.common.pojo;

import java.util.List;

import com.google.common.collect.Lists;
import com.tianchi.django.common.enums.Resource;
import lombok.*;
import org.apache.commons.collections4.ListUtils;

import static com.tianchi.django.common.enums.Resource.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Pod {

    private String podSn;//动态迁移数据中给出的，容器的唯一表示，便于参赛者定位容器。

    private String appName;

    private String group;

    private int gpu;

    private int cpu;

    private int ram;

    private int disk;

    private List<Integer> cpuIDs;//对应于node topologies 结构中的cpu，用于cpu分配

    public int value(Resource type) {
        return type == GPU ? gpu : (type == CPU ? cpu : (type == RAM ? ram : disk));
    }

    public Pod copy() {
        return Pod.builder()
            .podSn(podSn).appName(appName).group(group).gpu(gpu).cpu(cpu).ram(ram).disk(disk)
            .cpuIDs(Lists.newArrayList(ListUtils.emptyIfNull(cpuIDs))).build();
    }
}
