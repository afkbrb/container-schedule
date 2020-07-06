package com.tianchi.django;

import java.util.List;
import java.util.concurrent.*;

import com.tianchi.django.calculate.CalculateReschedule;
import com.tianchi.django.calculate.CalculateSchedule;
import com.tianchi.django.common.IReschedule;
import com.tianchi.django.common.ISchedule;
import com.tianchi.django.common.loader.source.*;
import com.tianchi.django.common.pojo.*;
import com.tianchi.django.common.pojo.associate.GroupRuleAssociate;
import com.tianchi.django.common.store.ResultStore;
import com.tianchi.django.common.utils.*;
import com.tianchi.django.pojo.ScheduleStatistic;
import com.tianchi.django.utils.DirectoryUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import static com.tianchi.django.common.constants.DjangoConstants.DEFAULT_TASK_COUNT;
import static com.tianchi.django.common.constants.DjangoConstants.MILLISECONDS_4_ONE_SEC;
import static com.tianchi.django.common.constants.DjangoConstants.MILLISECONDS_4_ONE_MIN;
import static java.util.stream.Collectors.toList;

@Slf4j
public class CalculateLauncher {

    /**
     * 运行静态布局和动态迁移功能
     * 使用其他目录数据，可以硬编码args = new String[] {"data_2"}做测试，或通过IDEA传参的方式启动main方法;
     */
    public static void main(String[] args) throws InterruptedException {

        long start = System.currentTimeMillis();

        List<String> directorys = DirectoryUtils.adjustDirectorys(args);

        execute(start, directorys);

        log.info("finish calculate, total use time : {}/s", (System.currentTimeMillis() - start) / MILLISECONDS_4_ONE_SEC);

        ScoreLauncher.main(args);

        Thread.sleep(2 * 60 * MILLISECONDS_4_ONE_MIN);//2小时避免程序退出
    }

    private static void execute(long start, @NonNull List<String> directories) throws InterruptedException {

        int taskCount = DEFAULT_TASK_COUNT * directories.size();;

        ExecutorService es = Executors.newFixedThreadPool(taskCount);//创建一个线程池，根据数据文件并行执行静态布局、动态迁移两个功能。

        CountDownLatch latch = new CountDownLatch(taskCount);//并发计数器,当任务线程都执行完后，程序才会退出。(当然程序执行额外会受到约束时间上限限制)

        long maxTimeLimitInMins = 0;

        for (String directory : directories) {

            Rule rule = new RuleSourceLoader(directory).load();//加载调度规则数据'rule.source'

            List<Node> nodes = new NodeSourceLoader(directory).load();//加载静态布局机器数据'schedule.node.source'

            List<App> apps = new AppSourceLoader(directory).load();//加载静态布局应用数据'schedule.app.source'

            List<NodeWithPod> nodeWithPods = new NodeWithPodSourceLoader(directory).load();//加载动态迁移原始数据'reschedule.source'

            maxTimeLimitInMins = Math.max(maxTimeLimitInMins, rule.getTimeLimitInMins());

            es.execute(() -> {
                try {

                    schedule(directory, start, rule, nodes, apps);

                } catch (Exception e) {

                    log.error(e.getMessage(), e);

                } finally {

                    latch.countDown();

                }
            });

            es.execute(() -> {
                try {

                    reschedule(directory, start, rule, nodeWithPods);

                } catch (Exception e) {

                    log.error(e.getMessage(), e);

                } finally {

                    latch.countDown();

                }
            });
        }

        latch.await(maxTimeLimitInMins + 1, TimeUnit.MINUTES);//docker运行也会卡时间。

        es.shutdownNow();

    }

    private static void schedule(String directory, long start, Rule rule, List<Node> nodes, List<App> apps) throws Exception {

        ISchedule iSchedule = new CalculateSchedule(start);

        log.info("{} | schedule source total score : {}", directory, ScoreUtils.resourceScore(nodes, rule));

        //执行静态布局功能
        List<ScheduleResult> results = iSchedule.schedule(
            nodes.parallelStream().map(Node::copy).collect(toList()), apps.parallelStream().map(App::copy).collect(toList()), rule.copy()
        );

        ResultStore.storeSchedule(results, directory);

        List<NodeWithPod> nodeWithPods = NodeWithPodUtils.resultToNodeWithPods(nodes, apps, results);

        ScheduleStatistic.from(directory, nodeWithPods, rule, GroupRuleAssociate.fromApps(apps)).log("schedule result");

    }

    private static void reschedule(String directory, long start, Rule rule, List<NodeWithPod> nodeWithPods) throws Exception {

        List<GroupRuleAssociate> groupRuleAssociates = GroupRuleAssociate.fromPods(NodeWithPodUtils.toPods(nodeWithPods));

        ScheduleStatistic.from(directory, nodeWithPods, rule, groupRuleAssociates).log("reschedule source");

        IReschedule iReschedule = new CalculateReschedule(start);

        //执行动态迁移功能
        List<RescheduleResult> results = iReschedule.reschedule(
            nodeWithPods.parallelStream().map(NodeWithPod::copy).collect(toList()), rule.copy()
        );

        ResultStore.storeReschedule(results, directory);

    }

}
