package com.hzw.monitor.mysqlbinlog.parser;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.event.EventData;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class NullEventDataParser implements EventDataParser {
	private static final Logger logger = LogManager.getLogger(NullEventDataParser.class);

	@Override
	public EventData parse(ByteBuf msg, ChannelHandlerContext context, int checksumLength) {
		LoggerUtils.debug(logger, "");
		return null;
	}

}
