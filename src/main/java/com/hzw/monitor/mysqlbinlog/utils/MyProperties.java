package com.hzw.monitor.mysqlbinlog.utils;

import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MyProperties {
	
	private static final Logger logger = LogManager.getLogger(MyProperties.class);

	// 以下为全局需要

	private static MyProperties myProperties = null;// 全局单例变量，一开始就存在

	public static void init(Properties props) {// 静态块里，只加载一次

		//Properties props = new Properties();
		//try {
		//	InputStream in = new BufferedInputStream(new FileInputStream(MyConstants.CONFIG_FILE));
			// Thread.currentThread().getContextClassLoader().getResourceAsStream(MyConstants.CONFIG_FILE);
		//	props.load(in);
		//	in.close();
		//} catch (Exception e) {
			// logger.error(e.toString());
		//	LoggerUtils.error(logger, "fail to read config file " + MyConstants.CONFIG_FILE);
		//	System.exit(-1);
		//}
		// 读取值mysql
		//LoggerUtils.debug(logger, "succeed to read config file " + MyConstants.CONFIG_FILE);
		
		// netty
		int netty_port = Integer.parseInt(props.getProperty(MyConstants.NETTY_PORT, "10000"));
		int netty_boss = Integer.parseInt(props.getProperty(MyConstants.NETTY_BOSS, "1"));
		int netty_worker = Runtime.getRuntime().availableProcessors()
				* Integer.parseInt(props.getProperty(MyConstants.NETTY_WORKER, "2").trim());// 2倍cpu
		// consumer
		int consumer_worker = Runtime.getRuntime().availableProcessors()
				* Integer.parseInt(props.getProperty(MyConstants.CONSUMER_WORKER, "2").trim());// 2倍cpu
		// zk
		String zk_servers = props.getProperty(MyConstants.ZK_SERVERS);
		
		int zk_session_timeout = Integer.parseInt(props.getProperty(MyConstants.ZK_SESSION_TIMEOUT, "5000"));
		int zk_retry_time = Integer.parseInt(props.getProperty(MyConstants.ZK_RETRY_TIME, "1000"));
		int zk_retry_max = Integer.parseInt(props.getProperty(MyConstants.ZK_RETRY_MAX, "3"));
		
		// countvalve
		long countValve = Long.parseLong(props.getProperty(MyConstants.COUNT_VALVE, "100"));
		props = null;
		// 构造新的对象
		myProperties = new MyProperties(netty_port, netty_boss, netty_worker, consumer_worker, zk_servers, countValve);
		myProperties.zk_session_timeout = zk_session_timeout;
		myProperties.zk_retry_time = zk_retry_time;
		myProperties.zk_retry_max = zk_retry_max;
		
		LoggerUtils.debug(logger, "succeed to create my properties object ");
	}

	public static MyProperties getInstance() {
		return myProperties;
	}

	// 私有属性开始//////////////////////////////////////////////////////////////////
	// netty
	private int netty_port;
	private int netty_boss;
	private int netty_worker;
	// consumer worker
	private int consumer_worker;
	// zk
	private String zk_servers;
	//
	private long accumalatedCountValue;
	
	private int zk_session_timeout = 5000;
	private int zk_retry_time = 1000;
	private int zk_retry_max = 3;
	

	private MyProperties() {// 私有方法，保证单例

	}

	private MyProperties(int np, int nboss, int nworker, int cworker, String zks, long countValue) {

		// used by netty
		this.netty_port = np;
		this.netty_boss = nboss;
		this.netty_worker = nworker;
		this.consumer_worker = cworker;
		this.zk_servers = zks;
		this.accumalatedCountValue = countValue;

	}

	public int getConsumer_Worker() {
		return this.consumer_worker;
	}

	public long getAccumalatedCountValue() {
		return accumalatedCountValue;
	}

	public String getZk_servers() {
		return zk_servers;
	}

	public int getNetty_port() {
		return netty_port;
	}

	public int getNetty_boss() {
		return netty_boss;
	}

	public int getNetty_worker() {
		return netty_worker;
	}

	public int getZk_session_timeout() {
		return zk_session_timeout;
	}

	public int getZk_retry_time() {
		return zk_retry_time;
	}

	public int getZk_retry_max() {
		return zk_retry_max;
	}

	public String toString() {
		StringBuilder strBuilder = new StringBuilder("\n");
		strBuilder.append(MyConstants.NETTY_PORT).append(": ").append(netty_port).append(" ");
		strBuilder.append(MyConstants.NETTY_BOSS).append(": ").append(netty_boss).append(" ");
		strBuilder.append(MyConstants.NETTY_WORKER).append(": ").append(netty_worker).append("\n");
		strBuilder.append(MyConstants.CONSUMER_WORKER).append(": ").append(consumer_worker).append("\n");
		strBuilder.append(MyConstants.ZK_SERVERS).append(": ").append(zk_servers).append("\n");
		strBuilder.append(MyConstants.COUNT_VALVE).append(": ").append(this.accumalatedCountValue).append("\n");
		return strBuilder.toString();
	}

	// 测试
	public static void main(String[] args) {
		// just for test
		MyProperties property = MyProperties.getInstance();
		logger.debug(property.toString());
	}
}
