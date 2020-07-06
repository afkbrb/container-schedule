package com.tianchi.django.common.loader.source;

import java.util.List;

import com.alibaba.fastjson.TypeReference;

import com.tianchi.django.common.constants.DjangoConstants;
import com.tianchi.django.common.loader.AbstractDataLoader;
import com.tianchi.django.common.pojo.App;
import com.tianchi.django.common.utils.JsonTools;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@AllArgsConstructor
public class AppSourceLoader extends AbstractDataLoader<List<App>> {//加载schedule.app.source原始数据

    private String directory;

    @Override
    public List<App> load() {

        String read = readFileToString(directory, DjangoConstants.SCHEDULE_APP_SOURCE);

        List<App> apps = JsonTools.parse(read, new TypeReference<List<App>>() {});

        log.info("{} | source count :【group {}, pod {}】", directory,
            apps.parallelStream().map(App::getGroup).distinct().count(), apps.parallelStream().mapToInt(App::getReplicas).sum());

        return apps;
    }
}
