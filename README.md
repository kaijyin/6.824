# 分布式课程-MIT6.824

#### 简介:    
 - MIT6.824是麻省理工的研究生公开课程,介绍了包括并行计算,一致性算法,分布式事务,缓存一致性,比特币等多种现代分布式技术,非常值得学习。
 - 完成分布式存储部分的所有实验(lab1-3),包括分片部分的三个challenge,共识算法通过多次测试(20000+)均无异常。
 - 课程主页:http://nil.csail.mit.edu/6.824/2021/schedule.html
 - 由于课程要求,找到实习后会设仓库为私有。 
###  实验：
 - lab1:实现MapReduce(未完成)
 - lab2:根据论文,设计并实现raft共识算法,完成领导人选举,日志/快照复制,快照压缩等功能
 - lab3:依靠下层raft设计并实现强一致性k-v存储,包括客户端(clint)和服务端集群(service).
 - lab4:再次改进,设计并实现数据分区以及分区控制功能,包括分区管理客户端与服务端集群,用户客户端与服务端集群。
#### 设计:
 - 项目采用Go语言原生RPC框架在单机上模拟真实网络环境。
 - 通过领导人选举采用**条件锁**进行控制。使用raft处理远端RPC请求采用**悲观锁**,避免并发错误。
 - K-V服务器与下层Raft通过**管道**传递日志,保证副本间日志一致性。
 - 所有客户端均编号处理,服务端维护客户端请求序号,保证**请求线性化**,避免重复写。
 - 所有到达服务端的请求均通过**时钟序号**标记,等待消费线程处理后再逐个清除,避免请求丢失。
 - 不同分组的集群主节点定期向分片控制服务器请求最新的**分区配置**(Shard Config),通过**Pull**的方式主动向原分区(Shard)所在组(集群)获取分区数据,并接管分区。

	 
	 
