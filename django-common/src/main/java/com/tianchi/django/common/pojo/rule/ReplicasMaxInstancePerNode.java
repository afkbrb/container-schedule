package com.tianchi.django.common.pojo.rule;

import com.tianchi.django.common.enums.Restrain;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReplicasMaxInstancePerNode {

    private int replicas;

    private Restrain restrain;//le && ge

    private int maxInstancePerNode;

    public ReplicasMaxInstancePerNode copy() {
        return ReplicasMaxInstancePerNode.builder()
            .replicas(replicas).restrain(restrain).maxInstancePerNode(maxInstancePerNode).build();
    }

}
