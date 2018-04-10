package com.hzw.monitor.mysqlbinlog.handlers;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.connection.ConnectionAttributes;
import com.hzw.monitor.mysqlbinlog.netty.server.MyNioSocketChannel;
import com.hzw.monitor.mysqlbinlog.utils.ByteUtils;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;
import com.hzw.monitor.mysqlbinlog.utils.NettyUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class FixedLengthHandlerV2 extends SimpleChannelInboundHandler<ByteBuf> {
	private static final Logger logger = LogManager.getLogger(FixedLengthHandlerV2.class);
	// public static AtomicBoolean valve = new AtomicBoolean(false);//
	// 是否要开启第一个字节检查开关
	// 格式: 3字节，然后1个位序号，后面为对应长度的字节
	// header
	// private byte[] header = new byte[5];
	private int[] header = new int[5];
	private int headerReaded = 0;
	// content
	private ByteBuf contentByteBuf = null;// 谁产生，谁释放,采用netty自身内存池加速
	private int contentLength = 0;
	private int contentReaded = 0;
	private boolean errorHappened = false;// 标记是否产生了错误，有错误需要退出

	private void trigger(ChannelHandlerContext context) throws Exception {// 本次读完了,是一个完整的报文

		if (contentReaded == contentLength) {// 完整报文
			// 提前拦截错误，防止发送到下游
			if (this.errorHappened) {
				// 打印
				LoggerUtils.error(logger, contentByteBuf.toString());
				// 确实有数据,就提取数据
				byte[] bytes = null;
				int length = 0;
				if (contentByteBuf.hasArray()) {// 支持数组方式
					bytes = contentByteBuf.array();
					length = bytes.length;
				} else {// 不支持数组方式
					length = contentByteBuf.readableBytes();
					bytes = new byte[length];
					contentByteBuf.getBytes(0, bytes);
				}
				LoggerUtils.error(logger, "" + new String(bytes));
				// 抛出异常，结束整个处理器
				throw new Exception("error happened...");
			}
			if (contentLength > 0) {// 有效报文
				context.fireChannelRead(contentByteBuf);
				// LoggerUtils.debug(logger,
				// "---------------------------------");
				// LoggerUtils.debug(logger, "trigger a complete packet...");
			}
			// 然后清空继续处理,开始下一轮数据请求
			// header = new byte[4];//这个可以复用
			headerReaded = 0;
			// contentByteBuf.release();// 不需要释放,加上反而会报错
			contentByteBuf = null;// 句柄也释放
			contentLength = 0;
			contentReaded = 0;
		}

	}

	@Override
	protected void channelRead0(ChannelHandlerContext context, ByteBuf msg) throws Exception {
		// 如果本链接已经异常，则直接关闭
		ConnectionAttributes myAttributes = ((MyNioSocketChannel) context.channel()).getAttributes();
		if (false == myAttributes.getGolbalValid().get()) {
			throw new Exception("false == myAttributes.getGolbalValid().get(),handle socket close event...");
		}
		//
		try {
			// LoggerUtils.debug(logger, "FixedLengthHandler2 channelRead0(...)
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
			// LoggerUtils.debug(logger, "length: " + length);
			// 处理每一个字节
			int index = 0;
			while (index < length) {
				if (0 == headerReaded) {
					// LoggerUtils.debug(logger, "first byte coming...");
					header[headerReaded++] = ByteUtils.verify(bytes[index++]);
				} else if (1 == headerReaded) {
					header[headerReaded++] = ByteUtils.verify(bytes[index++]);
				} else if (2 == headerReaded) {
					header[headerReaded++] = ByteUtils.verify(bytes[index++]);
					// LoggerUtils.debug(logger,
					// "header[2]:" + header[2] + " header[1]:" + header[1] + "
					// header[0]: " + header[0]);
					contentLength = ((header[2] * 256) + header[1]) * 256 + header[0] - 1;// 去掉后面的marker
					contentByteBuf = Unpooled.buffer(contentLength);
					// LoggerUtils.debug(logger, "this packet length expected:"
					// + contentLength);
				} else if (3 == headerReaded) {// sequence
					header[headerReaded++] = bytes[index++];// 不需要这个字节
				} else if (4 == headerReaded) {// marker必须要检查
					byte check = bytes[index++];
					LoggerUtils.debug(logger, "current headerReaded:" + headerReaded);
					LoggerUtils.debug(logger, "check: " + check);
					if (0XFF == check || -1 == check) {
						// 出错了
						LoggerUtils.error(logger, "error!!!error!!!error!!!   read message error, -1(0XFF) here...");
						errorHappened = true;
					}
					header[headerReaded++] = check;
					// 判断是否完整报文,防止有内容长度就是为0的情况的存在
					this.trigger(context);
				} else if (contentReaded == 0) {// 还没有填满//继续填充
					contentByteBuf.writeByte(bytes[index++]);
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
