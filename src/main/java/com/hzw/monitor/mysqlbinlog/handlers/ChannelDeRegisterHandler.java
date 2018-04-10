package com.hzw.monitor.mysqlbinlog.handlers;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;
import com.hzw.monitor.mysqlbinlog.utils.NettyUtils;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

public class ChannelDeRegisterHandler extends ChannelOutboundHandlerAdapter {
	private static final Logger logger = LogManager.getLogger(ChannelDeRegisterHandler.class);

	@Override
	public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
		LoggerUtils.debug(logger, "try to close socket by ChannelDeRegisterHandler.deregister()");
		NettyUtils.cleanChannelContext(ctx, null);
		// ctx.deregister(promise);
	}
}
