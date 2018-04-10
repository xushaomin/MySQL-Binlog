package com.hzw.monitor.mysqlbinlog.parser;

import com.hzw.monitor.mysqlbinlog.event.EventData;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public interface EventDataParser {
	public EventData parse(ByteBuf msg, ChannelHandlerContext context, int checksumLength) throws Exception;

}
