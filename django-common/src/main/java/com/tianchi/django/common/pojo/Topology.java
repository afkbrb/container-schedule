package com.tianchi.django.common.pojo;

import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Topology {

    private int socket;

    private int core;

    private int cpu;

    public Topology copy() {
        return Topology.builder().socket(socket).core(core).cpu(cpu).build();
    }

}
