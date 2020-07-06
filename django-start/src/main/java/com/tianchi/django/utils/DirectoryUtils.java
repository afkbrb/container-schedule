package com.tianchi.django.utils;

import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.tianchi.django.common.utils.JsonTools;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import static com.tianchi.django.common.constants.DjangoConstants.TEST_DIRECTORY;
import static java.util.stream.Collectors.toList;

@Slf4j
public class DirectoryUtils {

    /**
     * 根据args中的第一个参数或者默认目录(args为空)，确定多少个目录下的数据同时用于并行计算。
     */
    public static List<String> adjustDirectorys(String[] args) {

        //将会使用那几个目录下的数据并行用于计算
        List<String> directorys = ArrayUtils.isEmpty(args) ? ImmutableList.of(TEST_DIRECTORY) : Splitter.on(",").splitToList(args[0])
            .stream().map(StringUtils::defaultString).distinct().collect(toList());

        log.info("running data directory : {} ", JsonTools.toSimplifyJson(directorys));

        return directorys;

    }

}
