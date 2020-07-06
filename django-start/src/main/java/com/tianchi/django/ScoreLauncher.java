package com.tianchi.django;

import java.util.List;

import com.tianchi.django.common.loader.result.RescheduleResultLoader;
import com.tianchi.django.common.loader.result.ScheduleResultLoader;
import com.tianchi.django.common.loader.source.*;
import com.tianchi.django.common.pojo.*;
import com.tianchi.django.common.pojo.associate.GroupRuleAssociate;
import com.tianchi.django.common.pojo.associate.ScoreResult;
import com.tianchi.django.common.utils.*;
import com.tianchi.django.utils.DirectoryUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import static com.tianchi.django.common.constants.DjangoConstants.INVALID_SCORE;
import static com.tianchi.django.common.constants.DjangoConstants.MILLISECONDS_4_ONE_SEC;

@Slf4j
public class ScoreLauncher {

    /**
     * 评测静态布局、动态迁移功能
     * 使用其他目录数据，可以硬编码args = new String[] {"data_2"}做测试，或通过IDEA传参的方式启动main方法;
     */
    public static void main(String[] args) {

        long start = System.currentTimeMillis();

        List<String> directorys = DirectoryUtils.adjustDirectorys(args);

        execute(directorys);

        log.info("finish score, total use time : {}/s", (System.currentTimeMillis() - start) / MILLISECONDS_4_ONE_SEC);

    }

    /**
     * 串行执行评测，单目录费时秒级别。
     */
    private static void execute(@NonNull List<String> directorys) {

        long totalScheduleScore = 0, totalRescheduleScore = 0;

        for (String directory : directorys) {

            Rule rule = new RuleSourceLoader(directory).load();//加载调度规则数据'rule.source'

            List<Node> nodes = new NodeSourceLoader(directory).load();//加载静态布局机器数据'schedule.node.source'

            List<App> apps = new AppSourceLoader(directory).load();//加载静态布局应用数据'schedule.app.source'

            List<NodeWithPod> nodeWithPods = new NodeWithPodSourceLoader(directory).load();//加载动态迁移原始数据'reschedule.source'

            List<ScheduleResult> scheduleResults = new ScheduleResultLoader(directory).load();//加载schedule.result结果数据

            List<RescheduleResult> rescheduleResults = new RescheduleResultLoader(directory).load();//加载reschedule.result结果数据

            long scheduleScore = scheduleScore(directory, scheduleResults, rule, nodes, apps);//评测当前目录下静态布局功能

            //静态布局总分，若其中一个目录下的结果数据无效，整个静态布局总分无效。但依然后去计算所有的目录下的静态布局。
            if (scheduleScore == INVALID_SCORE || totalScheduleScore == INVALID_SCORE) {
                totalScheduleScore = INVALID_SCORE;
            } else {
                totalScheduleScore += scheduleScore;
            }

            long rescheduleScore = rescheduleScore(directory, rescheduleResults, rule, nodeWithPods);//评测当前目录下动态迁移功能

            if (rescheduleScore == INVALID_SCORE || totalRescheduleScore == INVALID_SCORE) {
                totalRescheduleScore = INVALID_SCORE;
            } else {
                totalRescheduleScore += rescheduleScore;
            }
        }

        log.info("ScoreResult:{}", JsonTools.toSimplifyJson(ScoreResult.builder().totalScheduleScore(totalScheduleScore).totalRescheduleScore(totalRescheduleScore).build()));

    }

    private static int scheduleScore(String directory, List<ScheduleResult> scheduleResults, Rule rule, List<Node> nodes, List<App> apps) {

        if (CollectionUtils.isEmpty(scheduleResults)) {
            return INVALID_SCORE;
        }

        List<NodeWithPod> nodeWithPods = NodeWithPodUtils.resultToNodeWithPods(nodes, apps, scheduleResults);

        int scheduleScore = ScoreUtils.scoreNodeWithPods(nodeWithPods, rule, GroupRuleAssociate.fromApps(apps));

        log.info("{} | schedule result total score : {}", directory, scheduleScore);

        return scheduleScore;
    }

    private static int rescheduleScore(String directory, List<RescheduleResult> rescheduleResults, Rule rule, List<NodeWithPod> nodeWithPods) {

        if (CollectionUtils.isEmpty(rescheduleResults)) {
            return INVALID_SCORE;
        }

        int rescheduleScore = ScoreUtils.scoreReschedule(rescheduleResults, rule, nodeWithPods);

        log.info("{} | reschedule result total score : {} ", directory, rescheduleScore);

        return rescheduleScore;

    }

}
