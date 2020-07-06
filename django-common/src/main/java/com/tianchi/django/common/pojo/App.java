package com.tianchi.django.common.pojo;

import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class App {

    private String appName;//应用名

    private String group;//应用分组

    private boolean cpuSensitive;//是否cpu敏感应用。用于node中topologies如何划分给相关pod

    private int gpu;

    private int cpu;

    private int ram;

    private int disk;

    private int replicas;//需要扩容的终态pod数量

    public App copy() {
        return App.builder()
            .appName(appName).group(group).cpuSensitive(cpuSensitive)
            .gpu(gpu).cpu(cpu).ram(ram).disk(disk).replicas(replicas).build();
    }
}
