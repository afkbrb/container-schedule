package com.tianchi.django.common.loader.result;

import java.util.List;

import com.alibaba.fastjson.TypeReference;

import com.google.common.collect.Lists;
import com.tianchi.django.common.constants.DjangoConstants;
import com.tianchi.django.common.loader.AbstractDataLoader;
import com.tianchi.django.common.pojo.ScheduleResult;
import com.tianchi.django.common.utils.JsonTools;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
@Getter
@AllArgsConstructor
public class ScheduleResultLoader extends AbstractDataLoader<List<ScheduleResult>> {//加载schedule.result结果数据

    private String directory;

    @Override
    public List<ScheduleResult> load() {

        String read = readFileToString(DjangoConstants.RESULT_DIRECTORY, directory, DjangoConstants.SCHEDULE_RESULT);

        if (StringUtils.isEmpty(read)) {
            return Lists.newArrayList();
        }

        return JsonTools.parse(read, new TypeReference<List<ScheduleResult>>() {});
    }

}
