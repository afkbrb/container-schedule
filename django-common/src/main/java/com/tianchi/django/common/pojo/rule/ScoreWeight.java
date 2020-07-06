package com.tianchi.django.common.pojo.rule;

import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScoreWeight {

    private int migratePod;//动态迁移，每迁移一个pod的权重分

    private int groupMoreInstancePerNode;//静态布局，单机同分组每多一个pod的权重分

    private int socketCross;//静态布局，单机单pod跨socket的cpu权重分

    private int coreBind;//静态布局，单机单pod同core绑定cpu权重分

    private int sensitiveCpuBind;//静态布局，单机多cpu敏感容器同core绑定cpu权重分

    public ScoreWeight copy() {
        return ScoreWeight.builder()
            .migratePod(migratePod).groupMoreInstancePerNode(groupMoreInstancePerNode)
            .socketCross(socketCross).coreBind(coreBind).sensitiveCpuBind(sensitiveCpuBind).build();
    }

}
