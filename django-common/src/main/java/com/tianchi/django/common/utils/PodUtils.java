package com.tianchi.django.common.utils;

import java.util.List;

import com.tianchi.django.common.enums.Resource;
import com.tianchi.django.common.pojo.App;
import com.tianchi.django.common.pojo.Pod;
import org.apache.commons.collections4.ListUtils;

public class PodUtils {

    /**
     * 通过app中的信息构建一个Pod对象
     */
    public static Pod toPod(App app) {
        return Pod.builder()
            .appName(app.getAppName()).group(app.getGroup())
            .gpu(app.getGpu()).cpu(app.getCpu()).ram(app.getRam()).disk(app.getDisk()).build();
    }

    /**
     * 计算一批Pod在某一项资源的总数
     */
    public static int totalResource(List<Pod> pods, Resource resource) {
        return ListUtils.emptyIfNull(pods).stream().mapToInt(pod -> pod.value(resource)).sum();
    }
}
