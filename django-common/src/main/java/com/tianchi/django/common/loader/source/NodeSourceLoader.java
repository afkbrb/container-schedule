package com.tianchi.django.common.loader.source;

import java.util.List;

import com.alibaba.fastjson.TypeReference;

import com.tianchi.django.common.constants.DjangoConstants;
import com.tianchi.django.common.loader.AbstractDataLoader;
import com.tianchi.django.common.pojo.Node;
import com.tianchi.django.common.utils.JsonTools;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@AllArgsConstructor
public class NodeSourceLoader extends AbstractDataLoader<List<Node>> {//加载schedule.node.source原始数据

    private String directory;

    @Override
    public List<Node> load() {

        String read = readFileToString(directory, DjangoConstants.SCHEDULE_NODE_SOURCE);

        List<Node> nodes = JsonTools.parse(read, new TypeReference<List<Node>>() {});

        log.info("{} | source node count : {}", directory, nodes.size());

        return nodes;
    }

}
