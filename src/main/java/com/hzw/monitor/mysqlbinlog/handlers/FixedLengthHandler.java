package com.hzw.monitor.mysqlbinlog.handlers;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 * @QQ: 837500869
 */
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.utils.ByteUtils;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;
import com.hzw.monitor.mysqlbinlog.utils.NettyUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class FixedLengthHandler extends SimpleChannelInboundHandler<ByteBuf> {
	private static final Logger logger = LogManager.getLogger(FixedLengthHandler.class);
	// public static AtomicBoolean valve = new AtomicBoolean(false);//
	// 是否要开启第一个字节检查开关
	// 格式: 3字节，然后1个位序号，后面为对应长度的字节
	// header
	private int[] header = new int[4];
	private int headerReaded = 0;
	// content
	private ByteBuf contentByteBuf = null;// 谁产生，谁释放,采用netty自身内存池加速
	private int contentLength = 0;
	private int contentReaded = 0;

	private void trigger(ChannelHandlerContext context) {// 本次读完了,是一个完整的报文
		if (contentReaded == contentLength) {// 完整报文
			if (contentLength > 0) {// 有效报文
				context.fireChannelRead(contentByteBuf);
			}
			// 然后清空继续处理,开始下一轮数据请求
			// header = new byte[4];//这个可以复用
			headerReaded = 0;
			// contentByteBuf.release();// 不需要释放,加上反而会报错
			contentByteBuf = null;// 句柄也释放
			contentLength = 0;
			contentReaded = 0;

			// LoggerUtils.info(logger,
			// "------------------------------------------------");
		}
	}

	@Override
	protected void channelRead0(ChannelHandlerContext context, ByteBuf msg) throws Exception {
		//
		try {
			// LoggerUtils.debug(logger, "\nFixedLengthHandler channelRead0(...)
			// ---");
			if (null == msg) {
				return;
			}
			// LoggerUtils.debug(logger, "Buffer type---" + msg.getClass());
			// 确实有数据,就提取数据
			byte[] bytes = null;
			int length = 0;
			if (msg.hasArray()) {// 支持数组方式
				bytes = msg.array();
				length = bytes.length;
			} else {// 不支持数组方式
				length = msg.readableBytes();
				bytes = new byte[length];
				msg.getBytes(0, bytes);
			}
			// 处理每一个字节
			int index = 0;
			while (index < length) {
				if (0 == headerReaded) {
					header[headerReaded++] = ByteUtils.verify(bytes[index++]);
				} else if (1 == headerReaded) {
					header[headerReaded++] = ByteUtils.verify(bytes[index++]);
				} else if (2 == headerReaded) {
					header[headerReaded++] = ByteUtils.verify(bytes[index++]);
					contentLength = ((header[2] * 256) + header[1]) * 256 + header[0];
					contentByteBuf = Unpooled.buffer(contentLength);
				} else if (3 == headerReaded) {
					header[headerReaded++] = bytes[index++];
					// 判断是否完整报文,防止有内容长度就是为0的情况的存在
					this.trigger(context);
				} else if (contentReaded == 0) {// 还没有填满,继续填充
					byte check = bytes[index++];
					// 第一个字节是否有效?
					if (0xFF == check) {// 参考:http://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
						// 出错了
						throw new Exception("read message error, -1(0XFF) here...");
					}
					contentByteBuf.writeByte(check);
					contentReaded++;
					this.trigger(context);
				} else {
					// 尽量一次性多读取一些字符
					int real = length - index;// 实际上剩下的可读内容
					int expected = contentLength - contentReaded;
					int readed = (expected <= real ? expected : real);
					contentByteBuf.writeBytes(bytes, index, readed);
					// 及时修改2个index指标
					index += readed;
					contentReaded += readed;
					// 判断是否完整报文
					this.trigger(context);
				}

			}
		} catch (Exception e) {
			LoggerUtils.error(logger, e.toString());
			throw new Exception(e);
		}
	}

	// msg.release();// 释放这个对象// 父类已经负责释放了,所以这里不需要释放// 本着“谁用谁释放”的原则

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		// Close the connection when an exception is raised.
		LoggerUtils.error(logger, cause.toString());
		NettyUtils.cleanChannelContext(ctx, cause);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		LoggerUtils.debug(logger, "[channelInactive] socket is closed by remote server");
		NettyUtils.cleanChannelContext(ctx, null);
	}

}
