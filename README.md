# 规模化容器静态布局和动态迁移

最终排名：2 / 4031，score = 501671，reschedule = 252009，schedule = 249662

赛题地址：https://tianchi.aliyun.com/competition/entrance/231791/information

官方赛题解析：https://tianchi.aliyun.com/forum/postDetail?postId=113204

## 说明

IDEA 需要安装 lombok 插件。

代码中有些硬编码的地方（比如一个阈值直接写死了），其实不太好，但由于已经明确不更换数据，我也懒得一般化了。

## 分析

分为 schedule 和 reschedule 两个部分。schedule 部分就是要在满足约束条件下，将容器（以下称为 pod）放入尽可能少的 node 中。reschedule 部分同样是要满足一定的约束，使用尽可能少的迁移次数对容器进行迁移，使 node 资源利用率尽可能的高。

每个 node 有一定的 cpu、ram、disk、gpu 和 eni 资源，由于赛题数据中每个 pod 需要消耗的 gpu 资源都是 0，可以忽略 gpu，类似地，题目中的 eni 资源和 disk 资源其实是相对充裕的，同样可以忽略。此处忽略不是指忽略约束（忽略约束的话直接报错，分数直接一刀 99999999（分数越低越好））而是指忽略其占用率。**通常都是 cpu 或 ram 不够用，我们只需要协调这两个资源就行了。**

## 思路

### schedule

采用贪心策略，每次处理一个 node，尽可能榨干其资源。

我一开始的思路如下：

```
设 toAllocatePods = { 所有需要分配资源的 pod }

foreach node
	allocatablePods = { toAllocatePods 中满足约束的所有能放入该 node 的 pod 组成的集合 }
    
	while (allocatablePods 非空)
		从 allocatablePods 中找出一个 bestFit // 规则见后
		将 bestFit 放入 node 中
		从 toAllocatePods 移除 bestFit
		重新计算 allocatablePods
```

获取 bestFit 步骤如下：对于 allocatablePods 中的每个元素，假设将这个 pod 加入 node，计算加入之后 node 的 cpu 和 ram 资源的离散系数，最终选取使得离散系数最小的那个 pod。

离散系数计算过程：假设某个 node 总共的 cpu 资源和 ram 资源分别为 R1, 和 R2，已经被消费的 cpu 和 ram 分别为 C1 和  C2，当前 pod 需要使用的 cpu 和 ram 就设为 cpu 和 ram，那么加入该 pod 后 cpu 的使用率 x = (C1 + cpu) / R1，gpu 使用率为 y = (C2 + gpu) / R2，容易推导离散系数如下：cov(x, y) = stdev(x, y) / avg(x, y) = ... = |x - y| / (x + y)


显然这题的关键在于要使得 cpu 和 ram 的利用率尽可能地接近，这样的话，两者能同时靠近 100%，从而减少一个资源先被耗尽导致另一个资源被浪费情况。

当然，使用方差/标准差也能达到这个目的，但使用离散系数的话在上式中 |x - y| 相同时会优先选取 x + y 更大的那一个，也就是说会优先放入比较消耗资源的 pod，从而使得剩余的 pod 大都是不太消耗资源的，增加了其能够插入 node 的可能性（集合 allocatablePods中的元素会更多）。

这个方法能将 schedule 分数刷到 26 万左右，观察日志可以发现尾部的很多 node 只插入了 1、2 个 pod。这是因为这些 pod 所属的 group 的 pod 实例太多，它们都被留到了最后，而又由于题目约束了一个 group 在单个 node 上可分配的 pod 实例上限（其实要么是 1 要么是 2），即使在某个 node 资源充足的情况下，pod 也不能插入该 node。

于是我将算法改成下下面这个：

```
设 toAllocatePods = { 所有需要分配资源的 pod }

foreach node
	allocatablePods = { toAllocatePods 中满足约束的所有能放入该 node 的 pod 组成的集合 }

	firstFits = { 从 allocatablePods 中按一定的规则找出一些需要一开始就放入 node 的 pods } // 规则后面会介绍
	将 firstFits 中的 pod 放入 node // 需要注意的是每放入一个 pod，就需要检查 firstFits 中的剩余的 pod 是否仍是可放入的
	将 firstFits 中的 pod 从 toAllocatePods 中移除
	放完 firstFits 后，重新计算 allocatablePods
	
	while (allocatablePods 非空)
		从 allocatablePods 中找出一个 bestFit // 规则见后
		将 bestFit 放入 node 中
		从 toAllocatePods 移除 bestFit
		重新计算 allocatablePods
```

我们增加了 firstFits 集合。firstFits 集合（大小为 1 或 2，详见代码）中的 pod 满足其所属的 group 的 pod 实例数是**当前剩余**的所有 group 中最多的。这样的话我们就成功地将原来那些被留到最后的 pod 提前解决了。将这些 pod 一开始就放入 node 的好处在于即使这些 pod 使得 node 的离散系数较大，算法后面会自动选取合适的 pod 来与这些 pod 形成削峰填谷的关系，使得离散系数变小。

经过此调整，分数成功达到 25 万左右（我太菜了，到现在还是没懂为什么每次分数会不同，不知道那里引入了随机性）。

### reschedule

有了 schedule 的经验，reschedule 就比较简单了。

官方 demo 已经给出了找到违背规则的 pod 并将其迁移使得布局合法的算法。

#### 第一步

一开始也复用该代码，先使布局合理再做进一步调整。

#### 第二步

定义 “不适合的 pod” 如下：设 pod 当前被放置在 node 中，令 x = pod 需要使用的 cpu / node 总共的 cpu，y = pod 需要使用的 ram / node 总共的 ram，如果 x，y 的离散系数大于一定阈值，我们就称这个 pod 为 “不合适的”。

调整的算法如下：

```
foreach node
	podsToMigrate = { 该 node 中所有不适合的 pod }
	将 podsToMigrate 中的每个 pod 移到后面的 node 中
	toAllocatePods = { 后面的所有 node 的 pod 中不适合的且能放入当前 node 的 pod 组成的集合 } // 后面和 schedule 很像
	allocatablePods = { toAllocatePods 中能够放入当前 node 的 pod 组成的集合 }
	
    while (allocatablePods 非空)
        从 allocatablePods 中找出一个 bestFit
    	将 bestFit 放入 node 中
    	从 toAllocatePods 移除 bestFit
    	重新计算 allocatablePods
```

#### 第三步

收尾。倒着遍历 node，尽可能地将后面 node 中的 pod 转移到前面来，以减少 node 使用数。

三步搞完，直接起飞，reschedule 分数达到了 252009，全场最佳。

## 一些投机的地方

规则要求代码能在半小时跑完就行了，我发现我一次 schedule 要跑 5min，一次 reschedule 要跑 5s，于是我每次运行就重复跑 schedule 和 reschedule 多次，选取最佳的组合，最终分数就好看些。

## 总结

之前打赛道一打了好久，分数一直不理想，换数据并取消日志后发现程序出现了 bug，但没日志根本没法找 bug，就全身心投入赛道二了，结果拿到了一个不错的成绩，确实是个惊喜。

另外，我的算法中的这些 bestFit、firstFit 的称谓是参考 [CSAPP malloc lab](https://github.com/afkbrb/csapp/tree/master/lab09-malloc/malloclab-handout) 的，CSAPP 诚不欺我！