package com.hzw.monitor.mysqlbinlog.utils;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.connection.ConnectionAttributes;
import com.hzw.monitor.mysqlbinlog.connection.ConnectionFactory;
import com.hzw.monitor.mysqlbinlog.netty.server.MyNioSocketChannel;
import com.hzw.monitor.mysqlbinlog.operation.OpeationEvent;
import com.hzw.monitor.mysqlbinlog.operation.OperationQueue;
import com.hzw.monitor.mysqlbinlog.operation.OperationType;
import com.hzw.monitor.mysqlbinlog.utils.StringUtils;
import com.hzw.monitor.mysqlbinlog.zookeeper.ZooKeeperUtils;

import io.netty.channel.ChannelHandlerContext;

public class NettyUtils {
	//
	private static final Logger logger = LogManager.getLogger(NettyUtils.class);

	// 清理channel的上下文环境
	public static void cleanChannelContext(ChannelHandlerContext ctx, Throwable cause) {
		ctx.close();
		ConnectionAttributes myAttributes = ((MyNioSocketChannel) ctx.channel()).getAttributes();
		// 及时更新到ZK,写到哪算哪,交给新开的Thread来写
		// ZooKeeperUtils.update(myAttributes.getBinlogPositionZKPath(),
		// myAttributes.getBinlogFileName() + ":"
		// + myAttributes.getBinlogPosition() + ":" +
		// System.currentTimeMillis());
		{
			myAttributes.setGolbalValid(false);// 通知其它人该停止处理本连接的数据
			try {
				myAttributes.getTaskThread().join();// 等待taskThread线程退出
			} catch (InterruptedException e) {
				LoggerUtils.error(logger, e.toString());
			}
		}
		// 发送一个消息给OperationServer,告诉它当前机器减少了一个连接
		OperationQueue.addObject(new OpeationEvent(null, OperationType.SOCKET_DELETE));
		LoggerUtils.debug(logger, "send a SOCKET_DELETE message to operation queue");
		// 通知其它人接力，如果需要的话
		ZooKeeperUtils.deletePath(myAttributes.getRunningZKPath());
		LoggerUtils.debug(logger, "channel is cleaned");
		// 最后才通知事件处理器，netty这边处理结束了
		ConnectionFactory.remove(StringUtils.union(myAttributes.getIp(), "" + myAttributes.getPort()));
	}

	// 主动触发此连接的非注册
	public static void triggerChannelClosed(MyNioSocketChannel mySocketChannel) {
		if (null == mySocketChannel) {
			return;
		}
		LoggerUtils.debug(logger, "triggerChannelClosed");
		// 这里必须设置
		mySocketChannel.getAttributes().setGolbalValid(false);
		// 这个runnable绝对是线程安全的 :)
		mySocketChannel.eventLoop().execute(new Runnable() {
			@Override
			public void run() {
				mySocketChannel.deregister();
				LoggerUtils.debug(logger, "deregister invoked");
			}
		});
	}

}
