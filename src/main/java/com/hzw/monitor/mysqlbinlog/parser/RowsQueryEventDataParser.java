package com.hzw.monitor.mysqlbinlog.parser;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.event.EventData;
import com.hzw.monitor.mysqlbinlog.event.data.RowsQueryEventData;
import com.hzw.monitor.mysqlbinlog.utils.ByteUtils;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * 
 * @author gqliu 2016年1月13日
 *
 */
public class RowsQueryEventDataParser implements EventDataParser {
	private static final Logger logger = LogManager.getLogger(RowsQueryEventDataParser.class);

	@Override
	public EventData parse(ByteBuf msg, ChannelHandlerContext context, int checksumLength) {
		LoggerUtils.debug(logger, "enter RowsQueryEventDataParser.parse(...)");
		int len = ByteUtils.readUnsignedInt(msg, 1);
		String queryStr = ByteUtils.readSpecifiedLengthString(msg, len);
		// 构造对象
		RowsQueryEventData eventData = new RowsQueryEventData();
		eventData.setQuery(queryStr);
		return eventData;
	}

}
