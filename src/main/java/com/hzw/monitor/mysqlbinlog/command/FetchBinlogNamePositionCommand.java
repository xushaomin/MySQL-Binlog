package com.hzw.monitor.mysqlbinlog.command;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.type.CommandType;
import com.hzw.monitor.mysqlbinlog.utils.ByteUtils;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

public class FetchBinlogNamePositionCommand {

	private static final Logger logger = LogManager.getLogger(FetchBinlogNamePositionCommand.class);
	
	private String sql = null;

	public FetchBinlogNamePositionCommand(String s) {
		this.sql = s;
	}

	public void write(ChannelHandlerContext context) {

		byte[] queryBytes = ByteUtils.writeByte((byte) CommandType.QUERY.ordinal(), 1);
		byte[] sqlBytes = this.sql.getBytes();// 不用带\0,所以不需要使用ByteUtils
		// // 构造总的数据
		int totalCount = queryBytes.length + sqlBytes.length;
		byte[] totalCountBytes = ByteUtils.writeInt(totalCount, 3);
		byte[] commandTypeBytes = new byte[1];
		commandTypeBytes[0] = 0;// 对于fetchBinlogNamePosition命令，这里就是0
		// 可以发送了
		ByteBuf finalBuf = Unpooled.buffer(totalCount + 4);
		finalBuf.writeBytes(totalCountBytes).writeBytes(commandTypeBytes).writeBytes(queryBytes).writeBytes(sqlBytes);
		context.channel().writeAndFlush(finalBuf);// 缓存清理
		LoggerUtils.info(logger, "send fetch binlog name&position succeed...");
	}
}
