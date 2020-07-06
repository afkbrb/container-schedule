package com.tianchi.django.common.pojo;

import java.util.List;

import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScheduleResult {//静态布局结果

    private String sn;//对应机器sn

    private String group;//对应一个应用分组

    private List<Integer> cpuIDs;//cpu分配

}
