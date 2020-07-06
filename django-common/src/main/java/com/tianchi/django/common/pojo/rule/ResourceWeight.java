package com.tianchi.django.common.pojo.rule;

import com.tianchi.django.common.enums.Resource;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ResourceWeight {

    private Resource resource;//宿主机资源类型

    private int weight;//资源打分权重

    private String nodeModelName;//宿主机机型

    public ResourceWeight copy() {
        return ResourceWeight.builder()
            .resource(resource).weight(weight).nodeModelName(nodeModelName).build();
    }

}
