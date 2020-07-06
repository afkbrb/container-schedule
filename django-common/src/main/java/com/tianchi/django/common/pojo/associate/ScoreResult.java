package com.tianchi.django.common.pojo.associate;

import lombok.*;

import static com.tianchi.django.common.constants.DjangoConstants.INVALID_SCORE;
import static com.tianchi.django.common.constants.DjangoConstants.INVALID_TOTAL_SCORE;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScoreResult {

    private long totalScheduleScore;

    private long totalRescheduleScore;

    private long score;

    public long getScore() {
        if (score > 0) {
            return score;
        }
        if (totalScheduleScore == INVALID_SCORE || totalRescheduleScore == INVALID_SCORE) {
            return INVALID_TOTAL_SCORE;
        } else {
            return totalScheduleScore + totalRescheduleScore;
        }
    }

}