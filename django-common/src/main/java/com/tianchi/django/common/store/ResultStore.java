package com.tianchi.django.common.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import com.google.common.base.Charsets;
import com.tianchi.django.common.pojo.*;
import com.tianchi.django.common.utils.JsonTools;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import static com.tianchi.django.common.constants.DjangoConstants.*;

@Slf4j
//此处数据存储在本地，后面会评测程序开发后，会上传到远端，因而storeSchedule和storeReschedule方法会修改。
public class ResultStore {

    public static void storeSchedule(@NonNull List<ScheduleResult> results, @NonNull String directory) throws IOException {

        String result = JsonTools.toSimplifyJson(results);

        File file = Paths.get(RESULT_DIRECTORY, directory, SCHEDULE_RESULT).toFile();

        log.info("schedule file : {} ,result count : {}", file.getAbsolutePath(), results.size());

        FileUtils.writeStringToFile(file, result, Charsets.UTF_8);

    }

    public static void storeReschedule(@NonNull List<RescheduleResult> results, @NonNull String directory) throws IOException {

        String result = JsonTools.toSimplifyJson(results);

        File file = Paths.get(RESULT_DIRECTORY, directory, RESCHEDULE_RESULT).toFile();

        log.info("reschedule file : {},result count : {}", file.getAbsolutePath(), results.size());

        FileUtils.writeStringToFile(file, result, Charsets.UTF_8);

    }

}
