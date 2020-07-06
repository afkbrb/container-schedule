package com.tianchi.django.pojo;

import lombok.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RescheduleStatistic {

    private int migrateCount;

    private ScheduleStatistic resultStatistic;

    public static RescheduleStatistic from() {
        return null;
    }

}
