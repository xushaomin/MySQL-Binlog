package com.hzw.monitor.mysqlbinlog.netty.server;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
import java.nio.channels.SocketChannel;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.connection.Connection;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;

import io.netty.channel.socket.nio.NioServerSocketChannel;

public class MyNioServerSocketChannel extends NioServerSocketChannel {
	private static final Logger logger = LogManager.getLogger(MyNioServerSocketChannel.class);

	// 继承已经有的类，用于干预连接
	@Override
	protected int doReadMessages(List<Object> buf) throws Exception {
		// logger.debug("\ndoReadMessages(...) enter....\n触发了新的连接...开始准备2阶段提取");
		// logger.debug("buf :" + buf);
		// LoggerUtils.debug(logger, new Exception().toString());
		// 原始部分,直接关闭
		try {
			SocketChannel tempCh = javaChannel().accept();
			tempCh.close();
		} catch (Exception e) {

		}

		// 移花接木
		Connection connection = NettyQueue.getObject();
		try {
			if (connection != null) {
				MyNioSocketChannel channel = new MyNioSocketChannel(this, connection.getSocketChannel(),
						connection.getAttributes());
				buf.add(channel);
				LoggerUtils.debug(logger, "db conn is as follows: " + connection.getSocketChannel());
				// logger.debug("buf :" + buf);
				// 成功了，这下可以放心了
				connection.getAttributes().setNettyManageState(1);// 成功了
				return 1;
			}
		} catch (Throwable t) {			
			LoggerUtils.error(logger, "Failed to create a new channel from an accepted socket." + t);
			try {
				connection.close();
			} catch (Throwable t2) {
				LoggerUtils.error(logger, "Failed to close a socket." + t2);
			}
			//设置失败
			connection.getAttributes().setNettyManageState(-1);// 失败了
		}
		return 0;

	}
}
