package com.tianchi.django.common.utils;

import java.util.*;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tianchi.django.common.enums.Resource;
import com.tianchi.django.common.enums.Restrain;
import com.tianchi.django.common.pojo.Rule;
import com.tianchi.django.common.pojo.associate.GroupRuleAssociate;
import com.tianchi.django.common.pojo.rule.ReplicasMaxInstancePerNode;
import com.tianchi.django.common.pojo.rule.ResourceWeight;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import static com.tianchi.django.common.constants.DjangoConstants.SCORE_EMPTY_NODE_MODEL_NAME;

@Slf4j
public class RuleUtils {

    /**
     * rule -> nodeResourceWeights数据转化为<资源,<机型,权重>>。
     */
    public static Map<Resource, Map<String, Integer>> toResourceScoreMap(Rule rule) {
        Map<Resource, Map<String, Integer>> nodeModelNameScoreMap = Maps.newHashMap();
        for (ResourceWeight rw : rule.getNodeResourceWeights()) {
            nodeModelNameScoreMap.computeIfAbsent(rw.getResource(), $ -> Maps.newHashMap())
                .put(StringUtils.defaultIfEmpty(rw.getNodeModelName(), SCORE_EMPTY_NODE_MODEL_NAME), rw.getWeight());
        }
        return nodeModelNameScoreMap;
    }

    /**
     * 返回所有应用<分组,分组对应的最大可堆叠实例数>
     */
    public static Map<String, Integer> toAllMaxInstancePerNodeLimit(Rule rule, List<GroupRuleAssociate> groupRuleAssociates) {

        Map<String, Integer> allMaxInstancePerNodeLimit = Maps.newConcurrentMap();

        for (ReplicasMaxInstancePerNode replicasMaxInstancePerNode : rule.getReplicasMaxInstancePerNodes()) {

            Restrain restrain = replicasMaxInstancePerNode.getRestrain();

            if (!EnumSet.of(Restrain.GE, Restrain.LE).contains(restrain)) {
                log.error("ReplicasMaxInstancePerNode rule error");
                continue;
            }

            int replicas = replicasMaxInstancePerNode.getReplicas();

            int maxInstancePerNode = replicasMaxInstancePerNode.getMaxInstancePerNode();

            if (restrain.equals(Restrain.GE)) {//此规则用意为应用实例数大于或等于(GE)replicas的应用，最大堆叠数量为maxInstancePerNode。
                groupRuleAssociates.parallelStream().filter(groupRuleAssociate -> groupRuleAssociate.getReplicas() >= replicas)
                    .forEach(groupRuleAssociate -> allMaxInstancePerNodeLimit.put(groupRuleAssociate.getGroup(), maxInstancePerNode));
                continue;
            }

            if (restrain.equals(Restrain.LE)) {
                groupRuleAssociates.parallelStream().filter(groupRuleAssociate -> groupRuleAssociate.getReplicas() <= replicas)//若存在冲突以最小的为准，实际不会出现这种情况。
                    .forEach(groupRuleAssociate -> allMaxInstancePerNodeLimit.compute(groupRuleAssociate.getGroup(),
                        (key, oldValue) -> oldValue == null ? maxInstancePerNode : Math.min(maxInstancePerNode, oldValue)));
            }

        }

        Set<String> existsRuleGroups = Sets.newHashSet(allMaxInstancePerNodeLimit.keySet());

        groupRuleAssociates.parallelStream().filter(groupRuleAssociate -> !existsRuleGroups.contains(groupRuleAssociate.getGroup()))//未存在规则的应用分组添加默认的堆叠上限。
            .forEach(groupRuleAssociate -> allMaxInstancePerNodeLimit.put(groupRuleAssociate.getGroup(), rule.getDefaultMaxInstancePerNode()));

        return allMaxInstancePerNodeLimit;

    }

}
