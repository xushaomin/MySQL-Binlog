package com.hzw.monitor.mysqlbinlog.server;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 * @qq:837500869
 */
import java.io.IOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.consumer.ConsumerMananger;
import com.hzw.monitor.mysqlbinlog.netty.server.NettyServer;
import com.hzw.monitor.mysqlbinlog.operation.OperationServer;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;
import com.hzw.monitor.mysqlbinlog.utils.MyConstants;
import com.hzw.monitor.mysqlbinlog.utils.SqlUtils;
import com.hzw.monitor.mysqlbinlog.utils.TimeUtils;
import com.hzw.monitor.mysqlbinlog.zookeeper.MachineListener;
import com.hzw.monitor.mysqlbinlog.zookeeper.TaskListener;
import com.hzw.monitor.mysqlbinlog.zookeeper.ZooKeeperUtils;

public class MyServer {
	private static final Logger logger = LogManager.getLogger(MyServer.class);

	static {
		// 加载1次就可以了,启动时加载mysql驱动程序，免得后面耗时
		SqlUtils.init();
		// 保证zk的任务节点存在
		ZooKeeperUtils.ensurePersistentPathWithNoValue(MyConstants.ZK_NAMESPACE_TASKS);
		// 保证zk的机器节点存在
		ZooKeeperUtils.ensurePersistentPathWithNoValue(MyConstants.ZK_NAMESPACE_MACHINES);
	}

	public static void main(String[] args) throws IOException {
		LoggerUtils.debug(logger, "system begins...");
		// -1)先启动消费线程组
		ConsumerMananger.start();
		//
		// 0先启动Netty服务
		NettyServer.start();
		//
		//
		// 1启动Operation服务
		OperationServer.start();
		LoggerUtils.debug(logger, "---------------------------------------");
		//
		//
		// 2
		// 2.1)注册machines监听器
		LoggerUtils.debug(logger, "register machines listener ");
		if (false == ZooKeeperUtils.registerChildListener(MyConstants.ZK_NAMESPACE_MACHINES,
				MachineListener.getInstance().getListener())) {
			LoggerUtils.error(logger, "fail to register task listener,system will exit(-1)");
			System.exit(-1);
		}
		//
		// 3注册tasks监听器,这个一定要放到最后，否则有问题
		LoggerUtils.debug(logger, "register task listener ");
		if (false == ZooKeeperUtils.registerChildListener(MyConstants.ZK_NAMESPACE_TASKS,
				TaskListener.getInstance().getListener())) {
			LoggerUtils.error(logger, "fail to register task listener,system will exit(-1)");
			System.exit(-1);
		}
		//
		//
		// 4本线程用来做前台线程
		while (true) {
			// 睡眠3秒
			TimeUtils.sleepSeconds(3);
		}
	}

}
