package com.hzw.monitor.mysqlbinlog.handlers;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.command.AuthenticateCommand;
import com.hzw.monitor.mysqlbinlog.utils.ByteUtils;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;
import com.hzw.monitor.mysqlbinlog.utils.NettyUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class GreetingPacketResultHandler extends SimpleChannelInboundHandler<ByteBuf> {
	private static final Logger logger = LogManager.getLogger(GreetingPacketResultHandler.class);

	@SuppressWarnings("unused")
	@Override
	protected void channelRead0(ChannelHandlerContext context, ByteBuf msg) throws Exception {
		//
		try {
			// 在这里处理收到的消息
			// LoggerUtils.debug(logger, "enter GreetingPacketHandler -
			// channelRead0 (...)");
			// LoggerUtils.debug(logger, "msg length:" + msg.readableBytes());
			// 收到了一个完整的Greeting Packet...
			if (null == msg) {
				return;
			}
			// 1第1个字节是版本号
			int protocolVersion = ByteUtils.readUnsignedByte(msg);// 一个字节
			// if ((byte) 0xFF == (byte) protocolVersion) {
			// LoggerUtils.error(logger, "wrong protocol version");
			// 出错必须关闭
			// }
			LoggerUtils.debug(logger, "mysql protocol version: " + protocolVersion);
			// LoggerUtils.debug(logger, "" + msg);

			// 2 第2部分是serverVersion,一个以\0结束的字符串
			String serverVersion = ByteUtils.readZeroTerminatedString(msg);
			LoggerUtils.debug(logger, "serverVersion:" + serverVersion);
			// LoggerUtils.debug(logger, "" + msg);

			// 3 接下来四个字节是threadId
			long threadId = ByteUtils.readUnsignedLong(msg, 4);
			LoggerUtils.debug(logger, "threadId: " + threadId);

			// 4第4部分是scramblePrefix,一个以\0结束的字符串
			String scramblePrefix = ByteUtils.readZeroTerminatedString(msg);
			// LoggerUtils.debug(logger, "scramblePrefix-" + scramblePrefix);

			// 5 接下来取2个字节
			int serverCapabilities = ByteUtils.readUnsignedInt(msg, 2);
			// LoggerUtils.debug(logger, "serverCapabilities-" +
			// serverCapabilities);
			// 6接下来取1个字节
			int serverCollation = ByteUtils.readUnsignedByte(msg);
			// LoggerUtils.debug(logger, "serverCollation: " + serverCollation);
			// 7 接下来2个字节为serverStatus
			int serverStatus = ByteUtils.readUnsignedInt(msg, 2);
			// LoggerUtils.debug(logger, "serverStatus: " + serverStatus);
			// 8 接下来保留13个字节
			msg.skipBytes(13);

			// 9接下来是一个以\0结尾的字符串
			String scrambleSuffix = ByteUtils.readZeroTerminatedString(msg);
			String scramble = scramblePrefix + scrambleSuffix;
			// LoggerUtils.debug(logger, "scramble: " + scramble);
			// 可能还有其它的内容，暂时忽略,节省开销
			// String others = ByteUtils.readZeroTerminatedString(msg);
			// 发送验证命令
			new AuthenticateCommand(scramble, serverCollation).write(context);
			context.pipeline().remove(this);// 完成使命，退出历史舞台
		} catch (Exception e) {
			LoggerUtils.error(logger, e.toString());
			throw new Exception(e);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		// Close the connection when an exception is raised.
		// cause.printStackTrace();//务必要关闭
		LoggerUtils.error(logger, cause.toString());
		NettyUtils.cleanChannelContext(ctx, cause);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		LoggerUtils.debug(logger, "[channelInactive] socket is closed by remote server");
		NettyUtils.cleanChannelContext(ctx, null);
	}

}
