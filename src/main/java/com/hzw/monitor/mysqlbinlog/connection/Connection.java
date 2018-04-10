package com.hzw.monitor.mysqlbinlog.connection;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Connection {
	private static final Logger logger = LogManager.getLogger(Connection.class);
	private SocketChannel socketChannel;
	private ConnectionAttributes attributes;

	public Connection(SocketChannel s, ConnectionAttributes a) {
		this.socketChannel = s;
		this.attributes = a;
	}

	public SocketChannel getSocketChannel() {
		return this.socketChannel;
	}

	public ConnectionAttributes getAttributes() {
		return attributes;
	}

	public void close() {
		if (null != socketChannel) {
			try {
				socketChannel.close();
				logger.info("close socket: " + socketChannel);
			} catch (IOException e) {
			}
			socketChannel = null;
		}

	}
}
