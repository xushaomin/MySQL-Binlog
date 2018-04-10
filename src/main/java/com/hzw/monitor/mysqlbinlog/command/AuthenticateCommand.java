package com.hzw.monitor.mysqlbinlog.command;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 * @QQ: 837500869
 */
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.connection.ConnectionAttributes;
import com.hzw.monitor.mysqlbinlog.netty.server.MyNioSocketChannel;
import com.hzw.monitor.mysqlbinlog.type.ClientCapabilitiesType;
import com.hzw.monitor.mysqlbinlog.utils.ByteUtils;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

public class AuthenticateCommand {
	// public static void main(String[] args) {
	// // ctx.channel().writeAndFlush();
	// }
	private static final Logger logger = LogManager.getLogger(AuthenticateCommand.class);
	private String scramble;
	private int collation;

	public AuthenticateCommand(String s, int c) {
		this.scramble = s;
		this.collation = c;
	}

	public void write(ChannelHandlerContext context) {
		// LoggerUtils.debug(logger, "AuthenticateCommand");
		// 准备第一部分
		int clientCapabilities = ClientCapabilitiesType.LONG_FLAG | ClientCapabilitiesType.PROTOCOL_41
				| ClientCapabilitiesType.SECURE_CONNECTION;
		byte[] clientCapabilitiesBytes = ByteUtils.writeInt(clientCapabilities, 4);
		// 准备第二部分
		int maxLength = 0;
		byte[] maxLengthBytes = ByteUtils.writeInt(maxLength, 4);
		// 准备第三部分
		byte[] collationBytes = ByteUtils.writeInt(this.collation, 1);
		// 准备第4部分
		byte[] zeroBytes = ByteUtils.writeInt(0, 23);
		// 第5部分，写用户名
		ConnectionAttributes myAttributes = ((MyNioSocketChannel) context.channel()).getAttributes();
		// ConnectionAttributes myAttribute =
		// context.channel().attr(MyConstants.MY_CONTEXT_ATTRIBUTES).get();
		String username = myAttributes.getUsernameForReplication();
		byte[] usernameBytes = ByteUtils.writeString(username);
		// 第6部分，准备密码
		byte[] lengthBytes = new byte[1];
		byte[] passwordBytes = null;
		String password = myAttributes.getPasswordForReplication();
		if (null == password || password.trim().length() == 0) {
			passwordBytes = new byte[0];// 空的
		} else {
			passwordBytes = ByteUtils.passwordCompatibleWithMySQL411(password, scramble);

		}
		lengthBytes[0] = (byte) passwordBytes.length;
		// 构造总的数据
		int totalCount = 0;
		totalCount += clientCapabilitiesBytes.length;
		totalCount += maxLengthBytes.length;
		totalCount += collationBytes.length;
		totalCount += zeroBytes.length;
		totalCount += usernameBytes.length;
		totalCount += lengthBytes.length;// 长度以一个字节发出去
		totalCount += passwordBytes.length;
		byte[] totalCountBytes = ByteUtils.writeInt(totalCount, 3);
		byte[] commandTypeBytes = new byte[1];
		commandTypeBytes[0] = 1;// 对于验证命令，这里就是1,其它都是0
		// 所有内容串联起来
		ByteBuf finalBuf = Unpooled.buffer(totalCount + 4);
		LoggerUtils.debug(logger, "---buffer---:" + finalBuf);
		finalBuf.writeBytes(totalCountBytes);
		finalBuf.writeBytes(commandTypeBytes);
		finalBuf.writeBytes(clientCapabilitiesBytes);
		finalBuf.writeBytes(maxLengthBytes);
		finalBuf.writeBytes(collationBytes);
		finalBuf.writeBytes(zeroBytes);
		finalBuf.writeBytes(usernameBytes);
		finalBuf.writeBytes(lengthBytes);
		finalBuf.writeBytes(passwordBytes);
		// 发射
		context.channel().writeAndFlush(finalBuf);// 缓存清理
		// finalBuf.release();//最终使用者负责释放了，不用担心
	}

}
