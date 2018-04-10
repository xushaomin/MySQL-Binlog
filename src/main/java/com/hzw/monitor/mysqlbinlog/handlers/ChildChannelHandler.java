package com.hzw.monitor.mysqlbinlog.handlers;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 * @email: 837500869@qq.com
 */
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.connection.ConnectionAttributes;
import com.hzw.monitor.mysqlbinlog.connection.ConnectionFactory;
import com.hzw.monitor.mysqlbinlog.netty.server.MyNioSocketChannel;
import com.hzw.monitor.mysqlbinlog.operation.OpeationEvent;
import com.hzw.monitor.mysqlbinlog.operation.OperationQueue;
import com.hzw.monitor.mysqlbinlog.operation.OperationType;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;
import com.hzw.monitor.mysqlbinlog.utils.MyConstants;
import com.hzw.monitor.mysqlbinlog.utils.StringUtils;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

public class ChildChannelHandler extends ChannelInitializer<SocketChannel> {
	private static final Logger logger = LogManager.getLogger(ChildChannelHandler.class);

	@Override
	protected void initChannel(SocketChannel channel) throws Exception {
		// 要在这里加上所有的处理句柄
		LoggerUtils.debug(logger, "initChannel invoked...");
		//LoggerUtils.debug(logger, "channel:" + channel.getClass().toGenericString());
		LoggerUtils.debug(logger, "channel:" + channel.getClass().getCanonicalName());
		ChannelPipeline cp = channel.pipeline();// 下面的次序不能变
		cp.addLast(MyConstants.FIXED_LENGTH_HANDLER, new FixedLengthHandler());
		cp.addLast(MyConstants.GREETING_PACKET_HANDLER, new GreetingPacketResultHandler());
		cp.addLast(MyConstants.AUTHEN_RESULT_HANDLER, new AuthenticateResultHandler());
		cp.addLast(MyConstants.FETCH_BINLOG_NAMEPOSITION_RESULT_HANDLER, new FetchBinlogNamePositionResultHandler());
		cp.addLast(MyConstants.FETCH_BINLOG_CHECKSUM_RESULT_HANDLER, new FetchBinlogChecksumResultHandler());
		cp.addLast(MyConstants.LOG_EVENT_PARSE_HANDLER, new BinlogEventParseHandler());
		cp.addLast(MyConstants.CHANNEL_TRIGGER_DE_REGISTERED, new ChannelDeRegisterHandler());
		// 到这里就可以存起来
		MyNioSocketChannel mySocketChannel = (MyNioSocketChannel) channel;
		ConnectionAttributes myAttributes = mySocketChannel.getAttributes();
		String ip = myAttributes.getIp();//这里是IP数组，没问题
		int port = myAttributes.getPort();
		ConnectionFactory.put(StringUtils.union(ip, "" + port), mySocketChannel);
		// 发送一个消息给OperationServer,告诉它当前机器新增了一个连接
		// 放入队列就闪
		OperationQueue.addObject(new OpeationEvent(null, OperationType.SOCKET_ADD));
		LoggerUtils.debug(logger, "send a SOCKET_ADD message to operation queue");
		// 启动此socketChannel的任务线程
		myAttributes.startTaskThread();
	}

}
