package com.tianchi.django.common.pojo;

import java.util.List;

import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RescheduleResult {//动态迁移DAG结果

    private int stage;//迁移步骤,从1开始。相同步骤下并行执行。

    private String podSn;//对应source机器上一个pod信息

    private String sourceSn;//对应迁移前源机器sn

    private String targetSn;//对应迁移后目标机器sn

    private List<Integer> cpuIDs;//迁移后目标机器上cpu分配

}
