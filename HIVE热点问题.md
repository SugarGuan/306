# # HIVE热点问题



Hive是基于Hadoop的数据仓库，它构建在Hadoop之上。（数据存储于HDFS中，数据计算使用MapReduce）。它将结构化的数据文件映射为一张数据库表，并提供完整的SQL查询功能（具体是将SQL转换成MapReduce任务执行~ SQL=>Hive SQL）



## 1、HIVE的应用场景？（适用于、特点）



Hive的出现允许用户在不了解MapReduce程序写法的情况下对格式化数据开展复杂分析工作。

Hive允许用户编写自己定义的函数UDF（User define function），用来在查询中使用。Hive有3种UDF，分别是UDF、UDAF（A=Aggregation）和UDTF（T=Table）。



### **HIVE使用场景** 主要是日志分析和海量结构化数据的离线分析。



### **Hive优点：**

（1）简单，提供了类SQL查询语言—HQL

（2）可扩展，为超大数据集设计了计算、扩展能力，一般情况下无需重启Hive服务就可以完成集群规模扩展

（3）提供统一的元数据管理

（4）延展性，Hive支持用户自定义函数，用户可以根据自己的需求来实现自己的函数

（5）容错性，具有良好的容错性，当集群中的一台节点出现问题时，SQL还可以完成查询操作



### **Hive缺点：**

（1）HQL表达能力有限

​		1）迭代式算法无法表达，如 PageRank 算法

​		2）数据挖掘方面支持度不足，如K-Means算法

（2）Hive效率比较低

​		1）Hive自动生成的MapReduce任务，不够智能，不是最优化

​		2）Hive调优比较困难，粒度较粗

（3）Hive可控性差   `原因见（2）- 1）`



## 2、HIVE使用流程？

**※ 其他的问法：HIVE SQL怎么转换成底层的MapReduce程序？**

提交查询（Hive SQL） 

=> HiveQL解释器（Parser）：将HQL解释为抽象语法树（AST，Abstract Semantic tree）

=> 语法分析（Semantic Analyzer）：将抽象语法树转换成查询块（QB，Query Block）

=> 逻辑查询计划生成器（Logic Plan Generator）：将查询快转换成逻辑查询计划（OP Tree）

=> 逻辑查询计划优化器（Logicial Optimizer）：将上一步逻辑查询计划（OP Tree）转换成最优逻辑查询计划

=> 物理查询计划生成器（Physical Plan Generator）：根据OPTree生成具体的MapReduce Task Tree

=> 物理查询计划优化器（Physical Optimizer）：将上一步Task Tree进行优化，生成最优的物理查询计划（Task Tree）

=> Hadoop引擎执行MapReduce任务，得出查询结果

List或者值；或文件输出



## 3、HIVE数据倾斜产生的原因，怎么解决？

当Hive出现以下情形时，认为Hive出现数据倾斜：

任务进度很快抵达一个较高的值（如95%），此时查看任务监控页面，发现只有少量（1~数）数量个**reduce**任务未完成。该reduce任务与其他reduce任务量差异巨大。

单一reduce任务记录数与平均记录数差异巨大（达到数量级），最长时长远远高于平均时长。

### 1、Hive数据倾斜产生的原因：

（1）key分配不均匀

（2）建表时考虑不周

（3）业务本身特性

（4）某些SQL语句本身就有数据倾斜



Hive数据倾斜多产生在以下HQL数据查询语句：

| 操作关键词     | 情形                                        | 后果                                             |
| -------------- | ------------------------------------------- | ------------------------------------------------ |
| Join           | 其中一个表较小，Key集中                     | 分发到某一个或某几个Reduce上的数据量远高于平均值 |
| Join           | 大表与大表，但是分桶的判断字段0值或空值过多 | 这些空值都由一个reduce处理，非常慢               |
| group by       | group by 维度过小 某值的数量过多            | 处理某值的reduce非常耗时                         |
| count distinct | 某特殊值过多                                | 处理某特殊值的reduce非常耗时                     |



### **2、数据倾斜的解决（或缓解）**

#### **（1）Join 大表与小表关联：**

a. 可以把  **重复关联键**少的表放在join**前面**做关联 (一般是小表) ； 这样可以降低reduce端触发操作次数，减少运行时间；

b. 可以同时使用Map Join让小的维度表缓存到内存。 在Map端完成Join过程。从而省略掉reduce端的工作。

修改属性

```XML
 hive.auto.convert.join=true
```

#### **（2）SQL语句造成数据倾斜**

##### **Join 大表与大表关联：**

a. 大表与大表关联时，若一张表中0或空值较多，容易将0和空值shuffle给一个reduce，造成任务运行缓慢。解决方式：为异常值赋一个随机值来分散key，均匀分配给多个reduce去执行

b. 当key值都是有效值时，解决方法：修改参数 

```xml
hiveoptimize.skeyjoin=true 
/** 和  **/
hive.skwejoin.key = skew_key_threshold
/** 默认值100000 **/
```

 以修改Hive空值倾斜的阈值。如果超过这个值，新的值会发给哪些还没有达到阈值的reduce。（一般需要对任务量进行预估，设置为平均reduce作业时间长度的2~4倍）

##### **group by情况：**

按照类型进行group by的时候，会将相同的group by字段的reduce任务需要的数据拉取到同一个节点进行聚合

解决方式：

```xml
hive.map.aggr=true（开启在map端聚合，类似于MR中的combiner操作）
hive.groupby.skewindata=true
```

##### **count distinct情况：**

count distinct时，将值为空的情况单独处理，

如果是计算count distinct，可以不用处理，直接过滤，在做后结果中加1。

如果还有其他计算，需要进行group by，可以先将值为空的记录单独处理，再和其他计算结果进行union.



**调节数据倾斜的终极目标是 ： 使map的输出数据更均匀的分布到reduce中去**



## 4、HIVE的shuffle过程？

和MapReduce的shuffle并没有区别。（见Hadoop热门问题.md 问题）



## 5、改造HIVE表后怎么进行数据一致性检验？ 有没有自动化流程？

Hive 的数据迁移方法包括HDFS的distcp功能或数据的导入导出。

**distcp方法**，如果集群建数据复制失败会给出报错提示信息。（一般无需对数据一致性进行校验）

**数据导入导出方式**需要将源数据导出到本地，然后在把数据复制至新集群节点下，并把数据load至集群。这种方法过程复杂，效率低下，工作量很大。但是在新老集群网络不通畅、集群间带宽很低的情况下，该方法是唯一的选择。（这种方法在数据传输过程中，产生数据丢失的可能性就会大大增加。）

**※MD5文件校验**（局限：仅仅适合于验证数据量不太大的表。如果数据量极大，排序速度缓慢，受限于磁盘IO和集群网络通信能力等制约，MD5计算效率极低。）



## 6、HIVE的分组方式

**row_number()**  是没有重复值的排序(即使两天记录相等也是不重复的),可以利用它来实现分页
**dense_rank()**    是连续排序,两个第二名仍然跟着第三名
**rank()**                是跳跃排序的,两个第二名下来就是第四名



## 7、将文件导入Hive表中的方式

**从本地导入**：

```
load data local inpath ‘/home/1.txt’ (overwrite)into table student;
```

**从HDFS导入**：

```
load data inpath ‘/user/hive/warehouse/1.txt’ (overwrite)into table student;
```

**查询导入**：

```
create table student1 as select * from student;    
```

 (也可以具体查询某项数据)

**查询结果导入**：

```
insert (overwrite) into table staff select * from track_log;
```



## 8、从Hive导出数据的方式

**导出到本地：**

```sql
insert overwrite local directory '/tmp/my/1' rom format delimited fields terminated by '\t' select * from staff;
```

（递归创建目录）

**导出到HDFS：**

```sql
insert overwrite directory '/user/root/myhdfs_dir' rom format delimited fields terminated by '\t' select * from staff;
```

**Bass shell 覆盖追加导出（pipeline）**

```shell
bin/hive -e "select * from staff;" > /tmp/myBackup.log
```

**Sqoop把Hive数据导出到外部**



## 9、请说明 Hive 中 sort by、order by、cluster by、distribute by 各代表什么意思？

这4个都是对查询结果排序的函数：

**order by**： 会对输入做全局排序，所以只有一个reducer（如果有多个reducer无法保证全局严格有序）

**sort by** ：不是全局排序，**其在数据进入reducer之前完成排序**

**distribute by**：按照指定的字段对数据进行划分，输出到不同的reduce中

**cluster by**：除了distribute by的功能外，还具有sort by的功能



## 10、Hive中split、coalesce以及collect_list的用法？

split函数将字符串转化为数组；

coalesce返回参数中的第一个非空值，如果所有值都为空，则返回空（null）

collect_list列出该字段所有的值（不去冲） 如： 

```sql
select collect_list(id) from table;
```



## 11、Hive底层如何存储数据？

### （1）Hive底层如何存储空值null？

null在hive底层默认使用“\N”存储。

在sqoop到mysql之前需要将null的数据加工成其他字符，否则sqoop将会报错。

### （2） Hive存储格式

Hive支持三种存储模式：

1、**textfile**：hive默认的存储格式

据可以使用任意分隔符进行分割，每一行为一条记录。默认无压缩，可以用gzip、snappy等方式对数据压缩，但是会造成无法对数据切分并行操作。

2、**sequencefile**：将HDFS上的数据进行二进制格式编码，存储进行压缩

存储进行了压缩，有利于减少IO，也是基于行存储。

3、**rcfile**：基于sequencefile存储，但是基于列存储。

列值通常重复值很多，所以更利于压缩。这种方式压缩率更高。它先对行进行分组 ，在对列进行合并。

select表中的一列数据时，只会对该列的数据进行处理，但是其他存储方式不论select几列会对所有数据都读取出来。

select全列时，rcfile反而不如sequencefile的性能高了。



## 12、HIVE（文件）压缩方式

Hive使用文件压缩存储技术后，可以有效降低集群内部数据传输量和数据传输的时间消耗。对应的，为实现压缩、解压，集群内机器也需要消耗额外时间进行数据压缩解压处理。

- 开启文件压缩，需要执行以下sql脚本

```sql
SET hive.exec.compress.intermediate=true --启用中间数据压缩
SET hive.exec.compress.output=true; -- 启用最终数据输出压缩
SET mapreduce.output.fileoutputformat.compress=true; --启用reduce输出压缩
SET mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.SnappyCodec --设置reduce输出压缩格式
SET mapreduce.map.output.compress=true; --启用map输入压缩
SET mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec；-- 设置map输出压缩格式
```

也可以使用hive-site.xml文件设置启用中间的数据压缩，配置文件略。

压缩包括**map输入压缩**、**map输出压缩**和**reduce输出压缩**三类。



## 13、 Hive内部表和外部表的区别？

### 1、创建表

创建内部表时，会将数据移动到数据仓库指向的路径；

创建外部表时，仅记录数据所在的路径，不对数据的位置做任何改变

### 2、删除表

删除表的时候，

内部表的元数据和数据会被一起删除；

外部表只删除元数据，不删除数据，这样外部表相对来说更加安全，数据组织更灵活，方便共享源数据。



## 14、分区和分桶的区别

### 分区

1、是指按照数据表的某列或某些列分为多个区，区从形式上可以理解为文件夹，比如我们要收集某个大型网站的日志数据，一个网站每天的日志数据存在同一张表上，由于每天会生成大量的日志，导致数据表的内容巨大，在查询时进行全表扫描耗费的资源非常多。
2、那其实这个情况下，我们可以按照日期对数据表进行分区，不同日期的数据存放在不同的分区，在查询时只要指定分区字段的值就可以直接从该分区查找

### 分桶

1、分桶是相对分区进行更细粒度的划分。
2、分桶将整个数据内容安装某列属性值得hash值进行区分，如要按照name属性分为3个桶，就是对name属性值的hash值对3取摸，按照取模结果对数据分桶。
3、如取模结果为0的数据记录存放到一个文件，取模为1的数据存放到一个文件，取模为2的数据存放到一个文件



## 15、HIVE优化

### 通用设置

hive.optimize.cp=true：列裁剪 

hive.optimize.prunner：分区裁剪 

hive.limit.optimize.enable=true：优化LIMIT n语句 

hive.limit.row.max.size=1000000： 

hive.limit.optimize.limit.file=10：最大文件数

### 本地模式(小任务)

1、job的输入数据大小必须小于参数：hive.exec.mode.local.auto.inputbytes.max(默认128MB) 

2、job的map数必须小于参数：hive.exec.mode.local.auto.tasks.max(默认4) 

3、job的reduce数必须为0或者1 

​	hive.exec.mode.local.auto.inputbytes.max=134217728 

​	hive.exec.mode.local.auto.tasks.max=4 

​	hive.exec.mode.local.auto=true 

​	hive.mapred.local.mem：本地模式启动的JVM内存大小

### 并发执行

hive.exec.parallel=true ，默认为false 

hive.exec.parallel.thread.number=8

### Strict Mode

hive.mapred.mode=true，严格模式不允许执行以下查询： 

1、分区表上没有指定了分区 

2、没有limit限制的order by语句 

3、笛卡尔积：JOIN时没有ON语句 

### 动态分区

hive.exec.dynamic.partition.mode=strict：该模式下必须指定一个静态分区 

hive.exec.max.dynamic.partitions=1000 

hive.exec.max.dynamic.partitions.pernode=100：在每一个mapper/reducer节点允许创建的最大分区数

DATANODE：dfs.datanode.max.xceivers=8192：允许DATANODE打开多少个文件

### 推测执行

mapred.map.tasks.speculative.execution=true 

mapred.reduce.tasks.speculative.execution=true 

hive.mapred.reduce.tasks.speculative.execution=true;

### 多个group by合并

hive.multigroupby.singlemar=true：当多个GROUP BY语句有相同的分组列，则会优化为一个MR任务

### 虚拟列 

hive.exec.rowoffset：是否提供虚拟列

### 分组 

1、两个聚集函数不能有不同的DISTINCT列，以下表达式是错误的： INSERT OVERWRITE TABLE pv_gender_agg SELECT pv_users.gender, count(DISTINCT pv_users.userid), count(DISTINCT pv_users.ip) FROM pv_users GROUP BY pv_users.gender; 

2、SELECT语句中只能有GROUP BY的列或者聚集函数。 

### Combiner聚合 

hive.map.aggr=true;在map中会做部分聚集操作，效率更高但需要更多的内存。 hive.groupby.mapaggr.checkinterval：在Map端进行聚合操作的条目数目

### 数据倾斜 

1、hive.groupby.skewindata=true：数据倾斜时负载均衡，当选项设定为true，生成的查询计划会有两个MRJob。

2、第一个MRJob 中，Map的输出结果集合会随机分布到Reduce中，每个Reduce做部分聚合操作，并输出结果，这样处理的结果是相同的GroupBy Key 

​	有可能被分发到不同的Reduce中，从而达到负载均衡的目的； 

3、第二个MRJob再根据预处理的数据结果按照GroupBy Key分布到Reduce中（这个过程可以保证相同的GroupBy Key被分布到同一个Reduce中），最后完成最终的聚合操作。 

### 排序

ORDER BY colName ASC/DESC 

hive.mapred.mode=strict时需要跟limit子句 

hive.mapred.mode=nonstrict时使用单个reduce完成排序 

SORT BY colName ASC/DESC ：每个reduce内排序 

DISTRIBUTE BY(子查询情况下使用 )：控制特定行应该到哪个reducer，并不保证reduce内数据的顺序

CLUSTER BY ：当SORT BY 、DISTRIBUTE BY使用相同的列时。

### 合并小文件

hive.merg.mapfiles=true：合并map输出
hive.merge.mapredfiles=false：合并reduce输出
hive.merge.size.per.task=256*1000*1000：合并文件的大小
hive.mergejob.maponly=true：如果支持CombineHiveInputFormat则生成只有Map的任务执行merge
hive.merge.smallfiles.avgsize=16000000：文件的平均大小小于该值时，会启动一个MR任务执行merge。

### 自定义map/reduce数目
#### 1、减少map数目：
　　set mapred.max.split.size
　　set mapred.min.split.size
　　set mapred.min.split.size.per.node
　　set mapred.min.split.size.per.rack
　　set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat

#### 2、增加map数目：
1、当input的文件都很大，任务逻辑复杂，map执行非常慢的时候，可以考虑增加Map数，来使得每个map处理的数据量减少，从而提高任务的执行效率。
2、假设有这样一个任务：
select data_desc, count(1), count(distinct id),sum(case when …),sum(case when ...),sum(…) from a group by data_desc
3、如果表a只有一个文件，大小为120M，但包含几千万的记录，如果用1个map去完成这个任务，肯定是比较耗时的，这种情况下，我们要考虑将这一个文件合理的拆分成多个，这样就可以用多个map任务去完成。
　　set mapred.reduce.tasks=10;
　　create table a_1 as select * from a distribute by rand(123);
4、这样会将a表的记录，随机的分散到包含10个文件的a_1表中，再用a_1代替上面sql中的a表，则会用10个map任务去完成。每个map任务处理大于12M（几百万记录）的数据，效率肯定会好很多。

#### 3、reduce数目设置：

1、参数1：hive.exec.reducers.bytes.per.reducer=1G：每个reduce任务处理的数据量
2、参数2：hive.exec.reducers.max=999(0.95 * TaskTracker数)：每个任务最大的reduce数目
3、reducer数=min(参数2,总输入数据量/参数1)
4、set mapred.reduce.tasks：每个任务默认的reduce数目。典型为0.99*reduce槽数，hive将其设置为-1，自动确定reduce数目。

### 使用索引：

hive.optimize.index.filter：自动使用索引
hive.optimize.index.groupby：使用聚合索引优化GROUP BY操作

## 16、HIVE 实践题

**1、海量数据分布在100台电脑中，想个办法高效统计出这批数据的TOP10**

```txt
方案1:
在每台电脑上求出TOP10，可以采用包含10个元素的堆完成(TOP10小，用最大堆，TOP10大，用最小堆)。
比如求TOP10大，我们首先取前10个元素调整成最小堆，如果发现，然后扫描后面的数据，并与堆顶元素比较，如果比堆顶元素大，那么用该元素替换堆顶，然后再调整为最小堆。
最后堆中的元素就是TOP10大。

方案2
求出每台电脑上的TOP10后，然后把这100台电脑上的TOP10组合起来，共1000个数据
再利用上面类似的方法求出TOP10就可以了。
```

**2、有10万个淘宝店铺，每个顾客访问任意一个店铺时都会生成一条访问日志。访问日志存储表为visit。其中访问用户ID字段名称为uid，访问的店铺字段名称为store，请用Hive统计每个店铺的UV。**

```sql
SELECT store,count(uid) AS uv 
FROM visit
GROUP BY store;
```

------

**3、有一亿个用户，存储在Users中，其中有用户唯一字段*uid*。用户年龄*age*和用户消费总消费金额*total*。请用*sql*或者*spark*按照用户年龄从大到小进行排序。如果年龄相同，则按照总消费金额从小到大顺序排序。**

```sql
SELECT * FROM Users
ORDER BY age DESC, total ASC;
/** 
最后的 ASC 可以省略
**/
```

------

**4、当前有用户人生阶段表*LifeStage*。有用户唯一ID字段*UID*，用户人生阶段字段*stage*，其中stage字段内容为各个人生阶段标签按照英文逗号分割的拼接内容。示例：**  *计划买车,已买房,*  **每个用户的内容不同。请使用Hive SQL统计每个人生阶段的用户量。**

```sql
SELECT stage_TMP,count(DISTINCT uid)
FROM LITERAL VIEW explode(split(stage, ",")) LifeStage_TMP as stage_TMP
GROUP BY stage_TMP;
```

------

**5、和 4 一致数据场景。但是LifeStage中每行数据存储一个用户的人生阶段数据。如：  **

一行数据UID字段：43，stage 字段内容为 已买房, ； 另一行数据UID：43，stage：计划买车,*    **这样的新整合数据，给出Hive SQL语句。**

输出示例：

```txt
43，已买房
43，计划买车
43，准备上学
```

回答：

```sql
SELECT uid, stage_tmp
FROM LifeStage LITERAL VIEW exploit(split(stage,',')) as stage_tmp
GROUP BY uid;
```

------

**6、求5分钟之内访问次数达到100次的用户。**

包含10亿数据的数据库visit，包含字段userid，dt（timestamp），url（string）。统计五分钟之内访问次数到达100次的用户。

```hive sql
/** 使用HIVE滑动窗口（时间滑动窗口）机制 **/
SELECT DISTINCT userid
FROM visit
WHERE dt-lag(dt,100)<=5*60*1000;
```

------







