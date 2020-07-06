package com.tianchi.django.common.pojo.associate;

import java.util.Collections;
import java.util.List;

import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PodPreAlloc {//用于预分配cpu给pod

    public static final PodPreAlloc EMPTY_SATISFY = PodPreAlloc.builder().satisfy(true).cpus(Collections.emptyList()).build();

    public static final PodPreAlloc EMPTY_NOT_SATISFY = PodPreAlloc.builder().satisfy(false).cpus(Collections.emptyList()).build();

    private boolean satisfy;//是否满足扩容诉求

    private List<Integer> cpus;//满足扩容诉求情况下预分配的cpu信息

}
