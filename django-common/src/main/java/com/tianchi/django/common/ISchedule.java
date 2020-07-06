package com.tianchi.django.common;

import java.util.List;

import com.tianchi.django.common.pojo.*;
import lombok.NonNull;

public interface ISchedule {

    List<ScheduleResult> schedule(@NonNull final List<Node> nodes, @NonNull final List<App> apps, @NonNull final Rule rule) throws Exception;

}
