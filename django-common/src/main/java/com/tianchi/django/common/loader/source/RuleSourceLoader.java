package com.tianchi.django.common.loader.source;

import com.tianchi.django.common.constants.DjangoConstants;
import com.tianchi.django.common.loader.AbstractDataLoader;
import com.tianchi.django.common.pojo.Rule;
import com.tianchi.django.common.utils.JsonTools;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@AllArgsConstructor
public class RuleSourceLoader extends AbstractDataLoader<Rule> {//加载rule.source原始数据

    private String directory;

    @Override
    public Rule load() {

        String read = readFileToString(directory, DjangoConstants.RULE_SOURCE);

        Rule rule = JsonTools.parse(read, Rule.class);

        log.info("{} | rule : {}", directory, JsonTools.toSimplifyJson(rule));

        return rule;

    }
}
