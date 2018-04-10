package com.hzw.monitor.mysqlbinlog.handlers;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
import java.util.ArrayList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.command.CheckBinlogChecksumCommand;
import com.hzw.monitor.mysqlbinlog.command.RequestBinaryLogCommand;
import com.hzw.monitor.mysqlbinlog.connection.ConnectionAttributes;
import com.hzw.monitor.mysqlbinlog.netty.server.MyNioSocketChannel;
import com.hzw.monitor.mysqlbinlog.packet.RowPacket;
import com.hzw.monitor.mysqlbinlog.type.ChecksumType;
import com.hzw.monitor.mysqlbinlog.utils.ByteUtils;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;
import com.hzw.monitor.mysqlbinlog.utils.MyConstants;
import com.hzw.monitor.mysqlbinlog.utils.NettyUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class FetchBinlogChecksumResultHandler extends SimpleChannelInboundHandler<ByteBuf> {
	private static final Logger logger = LogManager.getLogger(FetchBinlogChecksumResultHandler.class);
	private State currentState = State.NONE;
	private ArrayList<RowPacket> rowPackets = new ArrayList<RowPacket>();
	private ChecksumType checkSumType = ChecksumType.NONE;

	public FetchBinlogChecksumResultHandler() {

	}

	private enum State {
		NONE, // 一个都没收到
		WAITING_FOR_BEGIN, // 收到了第一个
		WAITING_FOR_END, // 接收我们需要的报文
		END, SECOND_STAGE// 第二阶段
	};

	@SuppressWarnings("incomplete-switch")
	@Override
	protected void channelRead0(ChannelHandlerContext context, ByteBuf msg) throws Exception {

		//
		try {
			// LoggerUtils.debug(logger,
			// "进入FetchBinlogChecksumResultHandler处理器");
			// LoggerUtils.debug(logger, "state---" + currentState.name());
			// 会陆续收到一些完整的字符报文
			// 需要根据当前状态决定操作
			short firstByte;

			switch (currentState) {
			case NONE:
				// 成功了,切换状态
				currentState = State.WAITING_FOR_BEGIN;
				break;
			case WAITING_FOR_BEGIN:
				firstByte = ByteUtils.readUnsignedByte(msg);
				if (0x00FE != firstByte) {
					// 收到了一个无效的,skip
				} else {
					// 接收到了开始信号
					currentState = State.WAITING_FOR_END;
				}
				break;
			case WAITING_FOR_END:
				// 在此过程中有效的报文是我们要的
				msg.markReaderIndex();
				firstByte = ByteUtils.readUnsignedByte(msg);
				if (0x00FE != firstByte) {
					// 有效的报文
					// 重置下
					msg.resetReaderIndex();// 恢复
					RowPacket rowPacket = new RowPacket(msg);
					rowPackets.add(rowPacket);
				} else {
					// @@@@@@@@@@@@@@@@@@@@@
					// 结束了
					currentState = State.END;
					if (rowPackets.size() > 0) {// 尽最大努力保存现场情况，然后肯定要退出context
						// 只需要第一个
						checkSumType = ChecksumType.valueOf(rowPackets.get(0).getValues(1).toUpperCase());
					} else {
						checkSumType = ChecksumType.NONE;
					}

					// checkSumType之后，有可能进入第二阶段
					if (ChecksumType.NONE != checkSumType) {
						new CheckBinlogChecksumCommand("set @master_binlog_checksum= @@global.binlog_checksum")
								.write(context);
						currentState = State.SECOND_STAGE;
					} else {
						// 立即发送请求二进制日志命令
						//MyProperties p = MyProperties.getInstance();
						ConnectionAttributes myAttributes = ((MyNioSocketChannel) context.channel()).getAttributes();
						String name = myAttributes.getBinlogFileName();
						long position = myAttributes.getBinlogPosition();
						new RequestBinaryLogCommand(myAttributes.getClientId(), name, position).write(context);
						LoggerUtils.debug(logger, "RequestBinaryLogCommand: " + name + ":" + position);
						// 这里要做一个变换
						context.pipeline().remove(this);// 完成使命，退出历史舞台
						context.pipeline().removeFirst();// 把第一个删除
						context.pipeline().addFirst(MyConstants.FIXED_LENGTH_HANDLER_V2, new FixedLengthHandlerV2());
					}
					// @@@@@@@@@@@@@@@@@@@@@
				}
				break;
			case SECOND_STAGE:
				// 进入第二阶段,表明接受了checksum,恭喜
				// 成功进入,则需要设置序列化为本checksum.
				// LoggerUtils.info(logger, "succeed to check checksum: " +
				// checkSumType);
				// 保留checkSumType
				ConnectionAttributes myAttributes = ((MyNioSocketChannel) context.channel()).getAttributes();
				myAttributes.setChecksumType(checkSumType);
				//
				// 立即发送请求二进制日志命令
				String name = myAttributes.getBinlogFileName();
				long position = myAttributes.getBinlogPosition();
				new RequestBinaryLogCommand(myAttributes.getClientId(), name, position).write(context);
				LoggerUtils.debug(logger, "RequestBinaryLogCommand: " + name + ":" + position);
				// 这里要做一个变换
				context.pipeline().remove(this);// 完成使命，退出历史舞台
				context.pipeline().removeFirst();// 把第一个删除
				context.pipeline().addFirst(MyConstants.FIXED_LENGTH_HANDLER_V2, new FixedLengthHandlerV2());
				break;
			}
		} catch (Exception e) {
			LoggerUtils.error(logger, e.toString());
			throw new Exception(e);
		}

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		// Close the connection when an exception is raised.
		// cause.printStackTrace();//务必要关闭
		LoggerUtils.error(logger, "Exception:" + cause.toString());
		NettyUtils.cleanChannelContext(ctx, cause);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		LoggerUtils.debug(logger, "[channelInactive] socket is closed by remote server");
		NettyUtils.cleanChannelContext(ctx, null);
	}

}
