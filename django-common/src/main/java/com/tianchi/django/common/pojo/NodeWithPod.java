package com.tianchi.django.common.pojo;

import java.util.List;

import lombok.*;
import org.apache.commons.collections4.ListUtils;

import static java.util.stream.Collectors.toList;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NodeWithPod {

    private Node node;//node信息

    private List<Pod> pods;//该宿主机上的pod信息

    public NodeWithPod copy() {
        return NodeWithPod.builder().node(node.copy())
            .pods(ListUtils.emptyIfNull(pods).stream().map(Pod::copy).collect(toList())).build();
    }
}
