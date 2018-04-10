#MySQL-Binlog


为什么要自己写binlog?


**提出问题：**

当初选型Canal,看了半天源码发现未能进入binlog解析核心。

直接用,碰到问题去群里问,还是自己造轮子,有问题秒定位?我选择了后者.


**分析问题：**

基于自己之前有2年+的TCP/IP报文解析经验，对网络和数据流很熟悉，

官方文档有binlog格式文档、github上有一些不错的binlog单线程纯解析软件可参考

于是萌发自己写个binlog解析软件的想法。

**解决问题：**

结合github上一个单线程的纯binlog解析软件，看懂源码之后从0开始动手。

做了NIO封装(基于Netty,by 刘志强),集群HA(基于ZooKeeper,by 刘志强),并行加速(by 刘志强),Web(SpringMVC,by 刘国庆)





关于MySQL的主从复制机制，原理如下：

![输入图片说明](http://git.oschina.net/uploads/images/2016/0504/064519_d70018b4_70679.jpeg "在这里输入图片标题")
![输入图片说明](https://gitee.com/uploads/images/2017/1027/164800_c59f5218_70679.png "6bFq81L.png")

而MySQL-Binlog，正是从网络层伪装成slave进行binlog拉取操作。

---
如果您用了这个软件觉得还不错的话，可以扫描下方的二维码，阁下的支持就是鄙人前进的动力！


<img src="http://git.oschina.net/uploads/images/2016/0504/100053_afaebf72_70679.png" width="200" height="200"/>

