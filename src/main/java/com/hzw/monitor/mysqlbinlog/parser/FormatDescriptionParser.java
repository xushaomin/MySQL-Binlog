package com.hzw.monitor.mysqlbinlog.parser;

import com.hzw.monitor.mysqlbinlog.event.EventData;
import com.hzw.monitor.mysqlbinlog.event.data.FormatDescriptionEventData;
import com.hzw.monitor.mysqlbinlog.utils.ByteUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class FormatDescriptionParser implements EventDataParser {

	@Override
	public EventData parse(ByteBuf msg, ChannelHandlerContext context, int checksumLength) {

		// TODO Auto-generated method stub
		int binlogVersion = ByteUtils.readUnsignedInt(msg, 2);
		String serverVersion = ByteUtils.readSpecifiedLengthString(msg, 50).trim();
		msg.skipBytes(4);
		int headerLength = ByteUtils.readUnsignedByte(msg);// 仅仅1个字节
		FormatDescriptionEventData eventData = new FormatDescriptionEventData();
		eventData.setBinlogVersion(binlogVersion);
		eventData.setServerVersion(serverVersion);
		eventData.setHeaderLength(headerLength);
		// LoggerUtils.debug(logger, eventData.toString());
		return eventData;
	}

}
