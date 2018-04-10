package com.hzw.monitor.mysqlbinlog.connection;

import java.io.IOException;
/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.netty.server.MyNioSocketChannel;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;

public class ConnectionFactory {
	
	private static final Logger logger = LogManager.getLogger(ConnectionFactory.class);
	
	private static ConcurrentHashMap<String, MyNioSocketChannel> SocketChannelHashMap = new ConcurrentHashMap<String, MyNioSocketChannel>();

	public static void put(String key, MyNioSocketChannel value) {
		SocketChannelHashMap.put(key, value);
	}

	public static MyNioSocketChannel get(String key) {
		return SocketChannelHashMap.get(key);
	}

	public static MyNioSocketChannel remove(String key) {
		return SocketChannelHashMap.remove(key);
	}

	private static class MySocketChannel {
		private SocketChannel sChannel;
		private String ip;

		public MySocketChannel(SocketChannel sc, String str) {
			sChannel = sc;
			ip = str;
		}

		public SocketChannel getChannel() {
			return sChannel;
		}

		public String getIp() {
			return ip;
		}

	}

	private static MySocketChannel makeChannel(String ipCollection, int port) {
		// 因为可能是多个ip
		MySocketChannel mySocketChannel = null;
		String[] ipArray = ipCollection.split(",");
		for (String ip : ipArray) {
			try {
				mySocketChannel = new MySocketChannel(SocketChannel.open(new InetSocketAddress(ip, port)), ip);
				mySocketChannel.getChannel().socket().setKeepAlive(true);
				break;
			} catch (IOException e) {
				mySocketChannel = null;
				LoggerUtils.error(logger, "fail to connect " + ip + ":" + port);
			}
		}
		return mySocketChannel;
	}

	public static Connection makeObject(String ips, int port, String data, String runningPath,
			String binlogPositionPath, String initialFilename, long initialPosition, long clientID) {
		Connection myConn = null;
		if (null != ips && port >= 0) {
			try {
				// 在这里创建具体的对象
				MySocketChannel sChannel = makeChannel(ips, port);
				if (null == sChannel) {
					throw new Exception("connect all target machines ,fail!!!");
				}
				sChannel.getChannel().configureBlocking(false);// 非阻塞
				myConn = new Connection(sChannel.getChannel(),
						ConnectionAttributes.parse(data).setIpPort(ips, port, sChannel.getIp())
								.setRunningZKPath(runningPath).setBinlogPositionZKPath(binlogPositionPath)
								.setClientId(clientID).updateBinlogNameAndPosition(initialFilename, initialPosition));
			} catch (Exception e) {
				myConn = null;
				LoggerUtils.error(logger, e.toString());
			}
		}
		// 无论如何，都返回连接，失敗則返回null
		return myConn;
	}

}
