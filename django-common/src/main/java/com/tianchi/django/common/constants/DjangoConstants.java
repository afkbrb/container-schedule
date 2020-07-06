package com.tianchi.django.common.constants;

public class DjangoConstants {

    public static final String TEST_DIRECTORY = "data_2";//第一份测试数据目录。

    public static final String SCHEDULE_APP_SOURCE = "schedule.app.source";//原始应用数据文件(静态布局功能)

    public static final String SCHEDULE_NODE_SOURCE = "schedule.node.source";//原始机器数据文件(静态布局功能)

    public static final String RESCHEDULE_SOURCE = "reschedule.source";//迁移前原始集群数据文件(动态迁移功能)

    public static final String RULE_SOURCE = "rule.source";//规则文件(静态布局、动态迁移功能)

    public static final String SCHEDULE_RESULT = "schedule.result";//静态布局功能结果输出数据文件

    public static final String RESCHEDULE_RESULT = "reschedule.result";//动态迁移功能结果输出数据文件

    public static final String RESULT_DIRECTORY = "_result";//静态布局、动态迁移结果文件输出目录

    public static final long MILLISECONDS_4_ONE_SEC = 1000;//1秒钟对应多少毫秒

    public static final long MILLISECONDS_4_ONE_MIN = 60 * MILLISECONDS_4_ONE_SEC;//一分钟对应多少毫秒

    public static final int DEFAULT_TASK_COUNT = 2;//静态布局和动态迁移两个任务，启动两个线程。

    //rule -> nodeResourceWeights中未指定机型或者机型为空的权重数据为默认资源权重，在map中定义一个内部key。
    public static final String SCORE_EMPTY_NODE_MODEL_NAME = "SCORE_EMPTY_NODE_MODEL_NAME";

    public static final int INVALID_SCORE = -1;//无效分数

    public static final int INVALID_TOTAL_SCORE = 99999999;//无效分数

}
