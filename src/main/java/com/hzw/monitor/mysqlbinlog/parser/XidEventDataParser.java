package com.hzw.monitor.mysqlbinlog.parser;

import com.hzw.monitor.mysqlbinlog.connection.ConnectionAttributes;
import com.hzw.monitor.mysqlbinlog.event.EventData;
import com.hzw.monitor.mysqlbinlog.event.data.XidEventData;
import com.hzw.monitor.mysqlbinlog.netty.server.MyNioSocketChannel;
import com.hzw.monitor.mysqlbinlog.utils.ByteUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class XidEventDataParser implements EventDataParser {// 提交数据

	@Override
	public EventData parse(ByteBuf msg, ChannelHandlerContext context, int checksumLength) {
		// LoggerUtils.debug(logger, "");
		long xid = ByteUtils.readUnsignedLong(msg, 8);
		XidEventData eventData = new XidEventData();
		eventData.setXid(xid);
		// 删除保留的tableMapData
		ConnectionAttributes myAttributes = ((MyNioSocketChannel) context.channel()).getAttributes();
		if (null != myAttributes) {
			myAttributes.deleteTableMapEventData();
		}
		return eventData;
	}

}
