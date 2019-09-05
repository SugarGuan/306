# # Spark热点问题



Spark是专为大规模数据处理而设计的快速通用的计算引擎。是由UC伯克利开源的类 MR通用并行计算框架。Spark拥有 Hadoop MapReduce所具有的的优点。但不同于MapReduce的是：Job中间输出结果可以保存在内存中，无需再读取 HDFS。



## 1、Spark包括哪些框架，都是做什么的：



**Spark Core**：Spark引擎基础框架，提供通用计算功能。

**Spark Streaming**：流处理框架，流式数据处理。

**Spark MLlib**：大数据机器学习框架，提供基础大数据机器学习算法包。（大数据机器学习）

**Spark GraphX**：图数据库处理框架。（构建知识图谱、关系图谱等，如天眼查）



## 2、 Spark术语（专有名词）



Spark任务运行模式：

**本地模式** ：

Local - 本地开发测试，还分为 Local单线程 和 Local多线程（local-cluster）

**集群模式** ：

Standalone - Spark自带的资源调度框架，支持完全分布式，由 ZooKeeper保证服务 HA。

Yarn - 常出现在 Spark使用中，但实际是资源调度平台。

Mesos - mesos资源调度平台。

Kubemetes - 资源调度平台。



数据：

**RDD**：弹性分布式数据集，是 Spark对分布式数据和计算的基本抽象。

**DataFrame**：（待补充）



应用 / 应用程序：

**Job**：是一组作用在 RDD上的具体任务。由多组 Stage组成。

**Stage**：每个 Job拆分的 task组成的一组任务，成为 Stage（也称TaskSet）

**Task**：运行在Executor 上的工作单元。	

**Application**：Spark 应用程序。由一个 Driver program和若干 Executor组成。

**Driver Program**：驱动器程序。运行 Application的main（）函数，并且创建 SparkContext。

**Executor**：是 Application运行在 Worker Node上的一个进程。该进程负责运行Task，并且将数据存在内存或者磁盘上。每个 Application都会申请各自的 Executor来处理任务。

**SparkContext**：是 Spark程序的入口。（由驱动器程序创建）负责调度运算资源，协调各 Worker Node上的Executor。

**Worker Node**：集群中可以运行Application代码的节点，能够运行一或多个 Executor进程。

**TaskScheduler**：将 Stage（TaskSet）的内容提交给 Worker node集群运行，并返回结果。

ClusterManager：在集群上获取资源的外部服务（如 Yarn、 Mesos）

一组作业中包含一个 Job；一个Job可以拆分成运行在多节点上的 Task。（运行在一个节点上的）一组Task构成Stage。



任务（运算）分类：

**Transformations**：转换，RDD  =>  RDD

**Action**：行动，返回一个结果



其他：

**DAGScheduler**：根据 Job不断优化、构建的有向运算图（执行步骤）

**TaskScheduler**：将TaskSet提交给Worker（集群）运行，每个Executor运行什么Task就是在此处分配的。



## 3、Spark的主从架构（各节点/机器身份）



根据 Yarn划分：

- **Resource Manager**：负责调度全体资源（包括机器节点、算力等）

- **Application Manager**：负责管理Application，向Resource Manager申请资源。

- **Node Manager**：负责单节点的管理，管理节点上正在执行的task（Taskset）



根据主从结构：

- **Application Manager**：运行着Driver Program的机器，它创建Spark Context对象，管理系统。

- **Worker Node**：负责执行task的机器节点。



## 4、 Spark启动流程

- 启动Zookeeper 

  ```shell
  zkServer.sh start
  ```

- 启动hdfs

  ```shell
  start-dfs.sh
  ```

- 启动Yarn

  ```shell
  start-yarn.sh
  ```

- 启动Spark

  ```shell
  ~spark/sbin/start-all.sh
  ```



JPS的进程一览：

| NO   | 进程名          | 说明                                       |
| ---- | --------------- | ------------------------------------------ |
| 1    | NameNode        | HDFS NNA或者 NNS                           |
| 2    | DataNode        | HDFS DataNode                              |
| 3    | Master          | Spark Master节点（备份节点需额外手动启动） |
| 4    | Worker          | Spark Worker节点                           |
| 5    | NodeManager     | Yarn Node节点管理进程                      |
| 6    | ResourceManager | Yarn 整体资源管理进程                      |
| 7    | Journal Node    | NN image的存储通信进程                     |
| 8    | QuorumPeerMain  | ZooKeeper集群进程                          |



## 5、 Spark RDD（原理）



RDD是弹性分布集。是Spark中的数据基本抽象。RDD代表的是一个不可变，可分区，里面元素可并行运算的集合。RDD具有数据流模型的特点：自动容错、位置感知性以及可伸缩性。

RDD是一个容错的、并行的数据结构。可以让用户显示地将数据存储到磁盘和内存中；并能够控制数据的分区。

**RDD允许用户在执行多个查询时显式地将工作集缓存在内存中**（ rdd.persist() ），后续的查询能够重用工作集，这极大地提升了查询速度。



### RDD的属性

（1）一组分片（Partition），即数据集的基本组成单位。对于RDD来说，每个分片都会被一个计算任务处理，并决定并行计算的粒度。用户可以在创建RDD时指定RDD的分片个数，如果没有指定，那么就会采用默认值。默认值就是程序所分配到的CPU Core的数目。

（2）一个计算每个分区的函数。Spark中RDD的计算是以分片为单位的，每个RDD都会实现compute函数以达到这个目的。compute函数会对迭代器进行复合，不需要保存每次计算的结果。

（3）RDD之间的依赖关系。RDD的每次转换都会生成一个新的RDD，所以RDD之间就会形成类似于流水线一样的前后依赖关系。在部分分区数据丢失时，Spark可以通过这个依赖关系重新计算丢失的分区数据，而不是对RDD的所有分区进行重新计算。

（4）一个Partitioner，即RDD的分片函数。当前Spark中实现了两种类型的分片函数，一个是基于哈希的HashPartitioner，另外一个是基于范围的RangePartitioner。只有对于于key-value的RDD，才会有Partitioner，非key-value的RDD的Parititioner的值是None。Partitioner函数不但决定了RDD本身的分片数量，也决定了parent RDD Shuffle输出时的分片数量。

（5）一个列表，存储存取每个Partition的优先位置（preferred location）。对于一个HDFS文件来说，这个列表保存的就是每个Partition所在的块的位置。按照“移动数据不如移动计算”的理念，Spark在进行任务调度的时候，会尽可能地将计算任务分配到其所要处理数据块的存储位置。



## 6、Spark 任务执行流程（提交方式）



（1）提交任务后，Resource Manager分配一台Node（Node上均含有Node Manager）。该节点Node作为Spark程序（Application）驱动器程序运行节点。它执行Driver Program（生成**Spark Context**），并向**Resource Manager**请求节点资源。

（2）**Resource Manager**分配 Node 并在 Node 上启动 **Executor** 进程；

​		 启动 StandaloneExecutorBackend后台进程，将Executor运行情况随心跳发送到**集群上**（如果驱动器程序不运行在集群内，向Resource Manager发送心跳，如果在集群内，直接向该节点发送心跳）；

（3）**SparkContext** 构建成DAG图，将DAG图分解成Stage，并把 Taskset 发送给Task Scheduler。

​          **Executor** 向**SparkContext**申请Task；

（4）**Task Scheduler** 将 <u>Task</u>发放给 **Executor**运行同时 **SparkContext**将<u>应用程序代码</u>发放给**Executor**；

（5）<u>Task</u> 在 **Executor**上运行，运行完毕释放所有资源。

![img](https://images2018.cnblogs.com/blog/1228818/201804/1228818-20180425172026316-1086206534.png)

Spark on YARN模式根据Driver在集群中的位置分为两种模式：一种是YARN-Client模式，另一种是YARN-Cluster（或称为YARN-Standalone模式）。

- Yarn Client模式：

  Driver在本地运行

- Yarn Cluster模式：

  Driver Application 在Yarn集群中运行

  

## 7、Spark如何划分Stage（宽依赖、窄依赖）



**1、宽依赖、窄依赖**

RDD之间有一系列的依赖关系，依赖关系分为窄依赖和宽依赖。

**宽依赖：**父RDD的分区被子RDD的多个分区使用   例如 groupByKey、reduceByKey、sortByKey等操作会产生宽依赖，**会产生shuffle**。

**窄依赖：**父RDD的每个分区都只被子RDD的一个分区使用  例如map、filter、union等操作会产生窄依赖。



**2、Stage**

Spark任务（Job）包括RDD的transformation和action操作两种，其中 transformation 操作涉及到 RDD 与 RDD 的转换，是**惰性执行**的。

Spark任务会根据 RDD 之间的依赖关系形成一个 DAG 有向无环图。这个执行DAG会提交给DAG scheduler，DAGScheduler会把DAG划分为相互依赖的多个Stage（Stage内任务存在依赖），划分Stage的依据是RDD之间的宽窄依赖。



Spark 对 Stage 的划分：

一个Job会被拆分为多组Task。（多个Stage，每组任务被称为一个Stage）



> 在Spark中存在 2 种 Task ，分别是shuffleMapTask和ResultTask。分类依据是输出内容。shuffleMapTask为shuffle函数输入内容，ResultTask输出result值。          =>
>
> **在Spark中存在 2 种 Stage，分别是shuffle之前的Stage 和 shuffle后的Stage。**
>
> `（result直接输出，不再做处理）`

示例：

1、rdd.parallize(1 to 10).foreach(println)

没有 shuffle，所以只有一个 stage

2、rdd.map(x => (x, 1)).reduceByKey(_ + _).foreach(println)

reduceByKey()是reduce操作，所以这里有一个shuffle。

有 shuffle ，所以 shuffle 前是一个 stage，shuffle到下一个shuffle之间是另一个stage。这里共2个stages。

综上：

对于窄依赖：由于partition依赖关系的确定性，partition的转换处理就可以在同一个线程里完成，窄依赖就被spark划分到同一个stage中；

对于宽依赖：只能等父shuffle处理完成后，下一个stage才能开始接下来的计算。没到一个宽依赖执行一次stage划分。

从后往前推，遇到宽依赖就断开（划出新stage），遇到窄依赖就将RDD加入该Stage中。

（map一般是窄依赖，reduce多是宽依赖）



## 8、MapReduce和Spark对比









## 9、Spark Streaming从Kafka读取数据的两种方式？具体代码？如何确保数据（指Kafka中日志）无丢失？







## 10、Spark的常见操作（总结，map、reduce常见）

1、Spark命令总结



2、Spark任务提交参数总结





## 11、数据倾斜Spark如何处理

**数据倾斜只会发生在shuffle过程中**

数据倾斜现象：

1、绝大多数task执行速度很快，但是少数（一部分）task执行速度很慢

2、原本能够正常执行的Spark作业，突然爆出OOM（内存溢出）异常。（由业务代码造成）

数据倾斜原理：

数据倾斜原因是因为shuffle时，必须将各个节点上相同的 key 拉取到某个节点上的一个 task 来进行处理（比如reduceByKey根据key进行操作）。如果出现个别task数据量与其他task有明显区别时，则认为出现了数据倾斜。（如reduceByKey操作中，一个Key值对应着100万条key-value对，其他key对应不及10条key-value对；则一个key-value执行shuffle时间严重影响和拖累了任务整体执行时长）

**数据倾斜只会发生在shuffle过程中**

解决数据倾斜，应该查看数据倾斜发生在哪些Stage中。如果使用yarn-client模式，可以查看本地log；如果使用yarn-cluster模式提交，可以用Spark Web UI来查看当前运行到了第几个Stage。（无论哪种提交模式都可以查看Spark Web UI中的task数据分配量）进一步**确定过是不是task分配的数据不均匀导致了数据倾斜**。

找到task、stage后，去**寻找对应的shuffle**代码



## 12、Spark 代码实现题：

Google page-rank算法、word count算法

