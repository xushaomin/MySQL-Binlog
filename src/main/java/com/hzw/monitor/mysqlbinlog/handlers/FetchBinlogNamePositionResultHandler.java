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

import com.hzw.monitor.mysqlbinlog.command.FetchBinlogChecksumCommand;
import com.hzw.monitor.mysqlbinlog.connection.ConnectionAttributes;
import com.hzw.monitor.mysqlbinlog.netty.server.MyNioSocketChannel;
import com.hzw.monitor.mysqlbinlog.packet.RowPacket;
import com.hzw.monitor.mysqlbinlog.utils.ByteUtils;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;
import com.hzw.monitor.mysqlbinlog.utils.NettyUtils;
import com.hzw.monitor.mysqlbinlog.zookeeper.ZooKeeperUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class FetchBinlogNamePositionResultHandler extends SimpleChannelInboundHandler<ByteBuf> {
	private static final Logger logger = LogManager.getLogger(FetchBinlogNamePositionResultHandler.class);
	private State currentState = State.NONE;
	private ArrayList<RowPacket> rowPackets = new ArrayList<RowPacket>();

	private enum State {
		NONE, // 一个都没收到
		WAITING_FOR_BEGIN, // 收到了第一个
		WAITING_FOR_END, // 接收我们需要的报文
		END// 结束了
	};

	@Override
	protected void channelRead0(ChannelHandlerContext context, ByteBuf msg) throws Exception {
		//
		try {
			// LoggerUtils.debug(logger,
			// "进入FetchBinlogNamePositionResultHandler处理器");
			// LoggerUtils.debug(logger, "state---" + currentState.name());
			// 会陆续收到一些完整的字符报文
			// 需要根据当前状态决定操作
			short firstByte;

			switch (currentState) {
			case NONE:
				// 收到了一个包
				firstByte = ByteUtils.readUnsignedByte(msg);
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
					// 结束了
					currentState = State.END;
				}
				break;
			default:
				break;
			}

			if (currentState == State.END) {
				// logger.debug("rowPackets.size---" + rowPackets.size());
				if (rowPackets.size() > 0) {// 尽最大努力保存现场情况，然后肯定要退出context
					// 只需要第一个
					RowPacket validPacket = rowPackets.get(0);
					String binlogFilename = validPacket.getValues(0);
					long binlogPosition = Long.parseLong(validPacket.getValues(1));
					// 必须调整binlogPosition
					if (binlogPosition < 4) {
						binlogPosition = 4;// 强制从4开始
					}
					// 立刻保留到ZK&内存里，双边保持一致
					ConnectionAttributes myAttributes = ((MyNioSocketChannel) context.channel()).getAttributes();
					ZooKeeperUtils.update(myAttributes.getBinlogPositionZKPath(),
							binlogFilename + ":" + binlogPosition + ":" + System.currentTimeMillis());
					myAttributes.updateBinlogNameAndPosition(binlogFilename, binlogPosition);
					LoggerUtils.debug(logger, "binlogFileName -- " + binlogFilename);
					LoggerUtils.debug(logger, "binlogPosition -- " + binlogPosition);
					// 退出之前,发送下一个命令
					new FetchBinlogChecksumCommand("show global variables like 'binlog_checksum'").write(context);
					// logger.debug("发送checksum查询命令结束");
					context.pipeline().remove(this);// 完成使命，退出历史舞台
				} else {
					// 巧妇难为无米之炊
					throw new Exception("rowPackets.size() = 0,exception");// 关闭了，连接关闭
				}

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
		LoggerUtils.error(logger, cause.toString());
		NettyUtils.cleanChannelContext(ctx, cause);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		LoggerUtils.debug(logger, "[channelInactive] socket is closed by remote server");
		NettyUtils.cleanChannelContext(ctx, null);
	}

}
