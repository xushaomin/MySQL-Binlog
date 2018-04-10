package com.hzw.monitor.mysqlbinlog.command;

import com.hzw.monitor.mysqlbinlog.type.CommandType;
import com.hzw.monitor.mysqlbinlog.utils.ByteUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

public class CheckBinlogChecksumCommand {

	private String sql = null;

	public CheckBinlogChecksumCommand(String s) {
		this.sql = s;
	}

	public void write(ChannelHandlerContext context) {
		//LoggerUtils.debug(logger, "write CheckBinlogChecksumCommand...");
		byte[] queryBytes = ByteUtils.writeByte((byte) CommandType.QUERY.ordinal(), 1);
		byte[] sqlBytes = this.sql.getBytes();// 不用带\0,所以不需要使用ByteUtils
		// 准备
		int totalCount = queryBytes.length + sqlBytes.length;
		byte[] totalCountBytes = ByteUtils.writeInt(totalCount, 3);
		byte[] commandTypeBytes = new byte[1];
		commandTypeBytes[0] = 0;
		//
		ByteBuf finalBuf = Unpooled.buffer(totalCount + 4);
		finalBuf.writeBytes(totalCountBytes).writeBytes(commandTypeBytes).writeBytes(queryBytes).writeBytes(sqlBytes);
		context.channel().writeAndFlush(finalBuf);// 缓存清理
		// LoggerUtils.debug(logger, "发送CheckBinlogChecksumCommand succeed...");

	}
}
