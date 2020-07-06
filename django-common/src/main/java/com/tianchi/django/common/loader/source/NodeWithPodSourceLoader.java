package com.tianchi.django.common.loader.source;

import java.util.List;

import com.alibaba.fastjson.TypeReference;

import com.tianchi.django.common.constants.DjangoConstants;
import com.tianchi.django.common.loader.AbstractDataLoader;
import com.tianchi.django.common.pojo.NodeWithPod;
import com.tianchi.django.common.utils.JsonTools;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@AllArgsConstructor
public class NodeWithPodSourceLoader extends AbstractDataLoader<List<NodeWithPod>> {//加载reschedule.source原始数据

    private String directory;

    @Override
    public List<NodeWithPod> load()  {

        String read = readFileToString(directory, DjangoConstants.RESCHEDULE_SOURCE);

        return JsonTools.parse(read, new TypeReference<List<NodeWithPod>>() {});
    }
}
