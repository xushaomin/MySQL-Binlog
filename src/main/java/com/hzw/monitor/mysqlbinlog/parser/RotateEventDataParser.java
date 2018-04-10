package com.hzw.monitor.mysqlbinlog.parser;

import com.hzw.monitor.mysqlbinlog.event.EventData;
import com.hzw.monitor.mysqlbinlog.event.data.RotateEventData;
import com.hzw.monitor.mysqlbinlog.utils.ByteUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class RotateEventDataParser implements EventDataParser {

	@Override
	public EventData parse(ByteBuf msg, ChannelHandlerContext context, int checksumLength) {
		// TODO Auto-generated method stub
		// LoggerUtils.debug(logger, "available:" + msg.readableBytes());
		long binlogPosition = ByteUtils.readUnsignedLong(msg, 8);
		String binlogFileName = ByteUtils.readSpecifiedLengthString(msg,
				ByteUtils.availableWithChecksumLength(msg, checksumLength));
		// 第一步及时保留到全局对象,供后续处理
		//ConnectionAttributes myAttributes = ((MyNioSocketChannel) context.channel()).getAttributes();
		// Logger
		RotateEventData eventData = new RotateEventData();
		eventData.setBinlogFilename(binlogFileName);
		eventData.setBinlogPosition(binlogPosition);
		return eventData;
	}

}
