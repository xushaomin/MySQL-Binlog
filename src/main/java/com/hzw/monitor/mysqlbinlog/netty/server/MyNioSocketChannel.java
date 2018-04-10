package com.hzw.monitor.mysqlbinlog.netty.server;

import java.nio.channels.SocketChannel;

import com.hzw.monitor.mysqlbinlog.connection.ConnectionAttributes;

import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class MyNioSocketChannel extends NioSocketChannel {
	ConnectionAttributes attributes;

	public MyNioSocketChannel(Channel parent, SocketChannel socket, ConnectionAttributes a) {
		super(parent, socket);
		attributes = a;
	}

	public ConnectionAttributes getAttributes() {
		return attributes;
	}

}
