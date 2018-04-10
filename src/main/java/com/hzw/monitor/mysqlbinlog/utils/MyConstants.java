package com.hzw.monitor.mysqlbinlog.utils;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
public class MyConstants {
	//// https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
	public static final int MAX_PACKET_LENGTH = 16777215;

	// 解析配置文件使用
	public static String CONFIG_FILE = System.getProperty("mysqlbinlogProperties",
			"src/main/resources/mysqlbinlog.properties");

	// netty使用
	public static String NETTY_PORT = "netty_server_port";
	public static String NETTY_BOSS = "netty_boss_number";
	public static String NETTY_WORKER = "netty_worker_number";

	// 消费者的线程数
	public static String CONSUMER_WORKER = "consumer_worker_number";

	// zookeeper使用
	public static String ZK_SERVERS = "zk_servers";
	public static String ZK_NAMESPACE = "kmonitor-mysql-binlog";
	public static String ZK_NAMESPACE_TASKS = "/" + ZK_NAMESPACE + "/tasks";
	public static String ZK_NAMESPACE_MACHINES = "/" + ZK_NAMESPACE + "/machines";
	// countvalve
	public static String COUNT_VALVE = "count_valve";

	// handler使用
	public static String FIXED_LENGTH_HANDLER = "FIXED_LENGTH_HANDLER";
	public static String FIXED_LENGTH_HANDLER_V2 = "FIXED_LENGTH_HANDLER_V2";
	public static String GREETING_PACKET_HANDLER = "Greeting_Packet_Handler";
	public static String AUTHEN_RESULT_HANDLER = "Authen_Result_handler";
	public static String FETCH_BINLOG_NAMEPOSITION_RESULT_HANDLER = "Fetch_Binlog_NamePosition_Result_Handler";
	public static String FETCH_BINLOG_CHECKSUM_RESULT_HANDLER = "Fetch_Binlog_CheckSum_ResultHandler";
	public static String LOG_EVENT_PARSE_HANDLER = "Log_Event_Parse_Handler";
	public static String CHANNEL_TRIGGER_DE_REGISTERED = "Channel_trigger_deregistered";

	// 增删改查的数据toJson时使用
	public static String UUID = "uuid";
	public static String DATABASE = "databaseName";
	public static String TABLE = "tableName";
	public static String ACTION_TYPE = "optType";
	public static String ACTION_WRITE = "INSERT";
	public static String ACTION_UPDATE = "UPDATE";
	public static String ACTION_DELETE = "DELETE";
	public static String ACTION_TIME = "optTime";

	// user for connection attributes
	public static String USERNAME_FOR_REPLICATION = "ur";
	public static String PASSWORD_FOR_REPLICATION = "pr";
	public static String USERNAME_FOR_SCHEMA = "us";
	public static String PASSWORD_FOR_SCHEMA = "ps";
	// 过滤库表专用
	public static String FILTER_RULES = "r";
	public static String DATABASE_FOR_FILTER_RULES = "d";
	public static String TABLE_FOR_FILTER_RULES = "t";
	// 分库分表专用
	public static String SHARDING_RULES = "sharding";
	public static String REGEX_DATABASE_FOR_SHARDING_RULES = "rd";
	public static String REGEX_TABLE_FOR_SHARDING_RULES = "rt";
	public static String TARGET_DATABASE_FOR_SHARDING_RULES = "d";
	public static String TARGET_TABLE_FOR_SHARDING_RULES = "t";
	//
	public static String BINLOG_FILE_NAME = "fn";
	public static String BINLOG_FILE_POSITION = "fp";
	public static String TIMESTAMP = "t";
	public static String STATE = "s";
	public static String CLIENTID = "clientID";
	////// state
	public static String UPDATE = "update";
	public static String NEW = "new";
	public static String UPDATE_DONE = "update_done";

	// 历史记录天数
	public static final long HISTORY_RECORD_DAY = 30;
	public static final long MILLISECOND_OF_ONE_DAY = 86400000;

}
