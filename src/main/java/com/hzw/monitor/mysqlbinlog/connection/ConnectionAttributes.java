package com.hzw.monitor.mysqlbinlog.connection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hzw.monitor.mysqlbinlog.event.data.TableMapEventData;
import com.hzw.monitor.mysqlbinlog.snapshot.SnapShot;
import com.hzw.monitor.mysqlbinlog.snapshot.SnapShotType;
import com.hzw.monitor.mysqlbinlog.type.ChecksumType;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;
import com.hzw.monitor.mysqlbinlog.utils.MyConstants;
import com.hzw.monitor.mysqlbinlog.utils.MyProperties;
import com.hzw.monitor.mysqlbinlog.utils.SqlUtils;
import com.hzw.monitor.mysqlbinlog.utils.StringUtils;
import com.hzw.monitor.mysqlbinlog.utils.TimeUtils;
import com.hzw.monitor.mysqlbinlog.utils.WriteResultUtils;
import com.hzw.monitor.mysqlbinlog.zookeeper.ZooKeeperUtils;

public class ConnectionAttributes {
	// 记录了连接远程连接的属性
	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	private static final Logger logger = LogManager.getLogger(ConnectionAttributes.class);
	private boolean firstFormatDescription = true;

	public boolean isFirstFormatDescription() {
		return firstFormatDescription;
	}

	public void setFirstFormatDescription(boolean firstFormatDescription) {
		this.firstFormatDescription = firstFormatDescription;
	}

	private int nettyManageState = 0;// 0：放入体系 1:成功 -1:失败
	public int getNettyManageState() {
		return nettyManageState;
	}

	public void setNettyManageState(int nettyManageState) {
		this.nettyManageState = nettyManageState;
	}

	private String runningZKPath; //// runningPath
	private String binlogPositionZKPath; // binlogPositionPath
	private long clientId;// clientId
	private String ip; // 主机
	private int port;
	private String realConnectedIP;// 多个IP前提下，真正连接的有效IP
	private String usernameForReplication; // 用于复制
	private String passwordForReplication;
	private String usernameForSchema; // 用于获取schemaS
	public String getRealConnectedIP() {
		return realConnectedIP;
	}

	private String passwordForSchema;
	private ChecksumType checksumType = ChecksumType.NONE; // checksum
	// 过滤专用
	private List<FilterRule> filterRules; // filter rules
	private HashMap<String, Boolean> learnedFilterRules = new HashMap<String, Boolean>();
	//
	//
	// 分库分表专用
	private List<ShardingRule> shardingRules;// shardingRules
	private HashMap<String, ShardingRule> learnedShardingRules = new HashMap<String, ShardingRule>();
	//
	// 用来保存上一次处理的位置
	private String binlogFileName = null; // binlog

	public HashMap<String, ShardingRule> getLearnedShardingRules() {
		return learnedShardingRules;
	}

	private long binlogPosition = 4;
	//
	//
	//
	// 用来处理ZK History的逻辑
	private Long currentPosition;// 本条数据的position
	private Long nextPosition;// 上条数据的的position
	private String historyPositionDay;// 上次存放position的日期: 20160126
	//
	//
	//
	// 全局有效标志,涉及到多个线程的交互，所以一定满足互斥
	// 默认为true,一旦为false,所有相关方应该立刻停止处理
	private AtomicBoolean golbalValid = new AtomicBoolean(true);

	public AtomicBoolean getGolbalValid() {
		return golbalValid;
	}

	public void setGolbalValid(Boolean gValid) {
		this.golbalValid.set(gValid);
	}

	public Long getCurrentPosition() {
		return currentPosition;
	}

	public void setCurrentPosition(Long currentPosition) {
		this.currentPosition = currentPosition;
	}

	public Long getNextPosition() {
		return nextPosition;
	}

	public void setNextPosition(Long nextPosition) {
		this.nextPosition = nextPosition;
	}

	public String getHistoryPositionDay() {
		return historyPositionDay;
	}

	public void setHistoryPositionDay(String historyPositionDay) {
		this.historyPositionDay = historyPositionDay;
	}

	// RunningPath
	public ConnectionAttributes setRunningZKPath(String runningPath) {
		this.runningZKPath = runningPath;
		return this;
	}

	public String getRunningZKPath() {
		return runningZKPath;
	}

	// BinlogPositionZKPath
	public ConnectionAttributes setBinlogPositionZKPath(String binlogPositionPath) {
		this.binlogPositionZKPath = binlogPositionPath;
		return this;
	}

	public String getBinlogPositionZKPath() {
		return binlogPositionZKPath;
	}

	// ClientId
	public ConnectionAttributes setClientId(long clientId) {
		this.clientId = clientId;
		LoggerUtils.debug(logger, "clientId:" + clientId);
		return this;
	}

	public long getClientId() {
		return clientId;
	}

	// IP&Port
	public ConnectionAttributes setIpPort(String ip, int port, String rcIP) {
		this.ip = ip;
		this.port = port;
		this.realConnectedIP = rcIP;
		return this;
	}

	public void setDatabaseTableColumnsMapping(HashMap<String, HashMap<String, String>> databaseTableColumnsMapping) {
		this.databaseTableColumnsMapping = databaseTableColumnsMapping;
	}

	public void setPrimaryKeysMapping(HashMap<String, ArrayList<String>> primaryKeysMapping) {
		this.primaryKeysMapping = primaryKeysMapping;
	}

	// username&password for replication
	public void setUsernameForReplication(String usernameForReplication) {
		this.usernameForReplication = usernameForReplication;
	}

	public String getUsernameForReplication() {
		return usernameForReplication;
	}

	public void setPasswordForReplication(String passwordForReplication) {
		this.passwordForReplication = passwordForReplication;
	}

	public String getPasswordForReplication() {
		return passwordForReplication;
	}

	// username&password for schema
	public void setUsernameForSchema(String usernameForSchema) {
		this.usernameForSchema = usernameForSchema;
	}

	public String getUsernameForSchema() {
		return usernameForSchema;
	}

	public void setPasswordForSchema(String passwordForSchema) {
		this.passwordForSchema = passwordForSchema;
	}

	public String getPasswordForSchema() {
		return passwordForSchema;
	}

	// checksum
	public void setChecksumType(ChecksumType checksumType) {
		this.checksumType = checksumType;
	}

	public ChecksumType getChecksumType() {
		return checksumType;
	}

	public long getChecksumLength() {
		return this.checksumType.getLength();
	}

	// rule
	public void setFilterRules(List<FilterRule> rules) {
		this.filterRules = rules;
	}

	// shardingRule
	public void setShardingRules(List<ShardingRule> rules) {
		this.shardingRules = rules;
	}

	public List<ShardingRule> getShardingRules() {
		return shardingRules;
	}

	public boolean acceptByFilter(String d, String t) {
		// 先看本地是否学习过
		String key = StringUtils.union(d, t);
		Boolean learnedResult = learnedFilterRules.get(key);
		if (null != learnedResult) {
			// LoggerUtils.debug(logger, "learned regex,return learned result");
			return learnedResult.booleanValue();
		}
		// LoggerUtils.debug(logger, "no learned regex,learn from origin rules:"
		// + rules);
		// 之前没有学习过，遍历本地rules
		boolean result = false;
		if (filterRules.isEmpty()) {
			result = true;
		} else {
			for (FilterRule rule : filterRules) {
				if (rule.accept(d, t)) {
					result = true;
					break;
				}
			}
		}
		// 返回前，存入学习结果[无论成功还是失败,都是学习结果]
		learnedFilterRules.put(key, result);
		return result;

	}

	public ShardingRule findShardingRule(String d, String t) {
		// 先看本地是否学习过
		String key = StringUtils.union(d, t);
		// 之前已经存在过结果，可能是一个规则，也可能是null[表示没有对应的分库分表规则]
		if (learnedShardingRules.containsKey(key)) {
			return learnedShardingRules.get(key);
		}
		// 之前没有学习过，遍历本地rules
		// LoggerUtils.debug(logger, "no learned regex,learn from origin rules:"
		// + rules);
		//
		ShardingRule result = null;
		for (ShardingRule rule : shardingRules) {
			if (rule.accept(d, t)) {
				result = rule;
				break;
			}
		}
		// 返回前，存入学习结果[无论成功还是null,都是学习结果]
		learnedShardingRules.put(key, result);
		//
		return result;
	}

	// accumalatedBinlogPositionCount
	// public long incrAccumalatedBinlogPositionCount() {
	// accumalatedBinlogPositionCount++;
	// return accumalatedBinlogPositionCount;
	// }

	// public void setAccumalatedBinlogPositionCount(int
	// accumalatedBinlogPositionCount) {
	// this.accumalatedBinlogPositionCount = accumalatedBinlogPositionCount;
	// }

	// public int getAccumalatedBinlogPositionCount() {
	// return accumalatedBinlogPositionCount;
	// }

	// binlogFileName
	public ConnectionAttributes updateBinlogNameAndPosition(String name, long position) {
		this.binlogFileName = name.trim();
		this.binlogPosition = position < 4 ? 4 : position;
		return this;
	}

	public String getBinlogFileName() {
		return binlogFileName;
	}

	public long getBinlogPosition() {
		if (binlogPosition < 4)
			binlogPosition = 4;
		return binlogPosition;
	}

	//
	//
	// 3)tableMapEventDatas
	// 临时性的事件处理,用完了应该立刻删除,防止内存占用过多
	private HashMap<Long, TableMapEventData> tableMapEventDatas = new HashMap<Long, TableMapEventData>();

	public void putTableMapEventData(long tableId, TableMapEventData data) {
		LoggerUtils.debug(logger,
				"tableId:" + tableId + " " + data.getDatabase() + " " + data.getTable() + data.getTableId());
		tableMapEventDatas.put(tableId, data);
	}

	public TableMapEventData getTableMapEventData(long tableId) {
		return tableMapEventDatas.get(tableId);
	}

	public void deleteTableMapEventData() {
		// 直接删除了
		tableMapEventDatas.clear();
	}

	// 4)保留本连接对应的数据库-表-列名的关系// 处理database-table-columns映射关系
	private HashMap<String, HashMap<String, String>> databaseTableColumnsMapping = new HashMap<String, HashMap<String, String>>();
	// 记载了每个表的主键关系,如果没有，则应该为空
	private HashMap<String, ArrayList<String>> primaryKeysMapping = new HashMap<String, ArrayList<String>>();

	public HashMap<String, ArrayList<String>> getPrimaryKeysMapping() {
		return primaryKeysMapping;
	}

	public ArrayList<String> getPrimaryKeys(String database, String table) {
		String key = StringUtils.union(database, table);
		return primaryKeysMapping.get(key);
	}

	public void ensureDatabaseTableColumnsMappingDeleted(String database, String table) {
		String key = StringUtils.union(database, table);
		this.databaseTableColumnsMapping.remove(key);
		this.primaryKeysMapping.remove(key);
	}

	public void ensureDatabaseTableColumnsMappingExist(String database, String table, boolean forceUpdate)
			throws Exception {
		// LoggerUtils.info(logger, "[" + database + "][" + table + "]
		// forceUpdate:" + forceUpdate);
		// 因为比较耗时，所幸,并不是经常改数据表结构
		// 如果之前拉过一次，大部分情况后面不需要再重新拉取
		String key = StringUtils.union(database, table);
		if (false == forceUpdate) {// 不强制更新，有就行了
			if (null != databaseTableColumnsMapping.get(key)) {
				// 已经有了，不用做其它操作
				// LoggerUtils.debug(logger, "mappings已经存在，不用更新");
			} else {
				HashMap<String, String> mappings = SqlUtils.getDatabaseTableColumnsMapping(realConnectedIP, port,
						this.usernameForSchema, this.passwordForSchema, database, table, primaryKeysMapping);
				// LoggerUtils.debug(logger, "非强制更新" + mappings);
				databaseTableColumnsMapping.put(key, mappings);
			}
		} else {
			// 强制更新,拉取强制更新,不管有没有，一律强制更新
			// 比如修改了表结构[这种情况也不多,没事修改表结构干嘛。。。:)]
			HashMap<String, String> mappings = SqlUtils.getDatabaseTableColumnsMapping(realConnectedIP, port,
					usernameForSchema, passwordForSchema, database, table, primaryKeysMapping);
			// LoggerUtils.debug(logger, "强制更新" + mappings);
			// put会替换原有的值
			databaseTableColumnsMapping.put(key, mappings);
		}
	}

	public HashMap<String, HashMap<String, String>> getDatabaseTableColumnsMapping() {
		return databaseTableColumnsMapping;
	}

	public HashMap<String, String> getColumnsMapping(String database, String table) {
		return databaseTableColumnsMapping.get(StringUtils.union(database, table));
	}

	public static ConnectionAttributes parse(String data) {
		ConnectionAttributes attributes = null;
		// 提取出各种k/v
		JSONObject jsonObject = JSON.parseObject(data);
		String ur = jsonObject.getString(MyConstants.USERNAME_FOR_REPLICATION);
		String pr = jsonObject.getString(MyConstants.PASSWORD_FOR_REPLICATION);
		String us = jsonObject.getString(MyConstants.USERNAME_FOR_SCHEMA);
		String ps = jsonObject.getString(MyConstants.PASSWORD_FOR_SCHEMA);
		// 开始构建库表过滤规则
		ArrayList<FilterRule> tempFilterRules = new ArrayList<FilterRule>();
		{
			JSONArray rulesArray = jsonObject.getJSONArray(MyConstants.FILTER_RULES);
			JSONObject rule;
			String database;
			String table;
			for (Object obj : rulesArray) {
				rule = (JSONObject) obj;
				database = rule.getString(MyConstants.DATABASE_FOR_FILTER_RULES);
				table = rule.getString(MyConstants.TABLE_FOR_FILTER_RULES);
				tempFilterRules.add(new FilterRule(Pattern.compile(database), Pattern.compile(table)));
			}
		}
		// 开始构建分库分表规则
		ArrayList<ShardingRule> tempShardingRules = new ArrayList<ShardingRule>();
		{
			JSONArray rulesArray = jsonObject.getJSONArray(MyConstants.SHARDING_RULES);
			JSONObject rule;
			String regexDatabase;
			String regexTable;
			String targetDatabase;
			String targetTable;
			for (Object obj : rulesArray) {
				rule = (JSONObject) obj;
				regexDatabase = rule.getString(MyConstants.REGEX_DATABASE_FOR_SHARDING_RULES);
				regexTable = rule.getString(MyConstants.REGEX_TABLE_FOR_SHARDING_RULES);
				targetDatabase = rule.getString(MyConstants.TARGET_DATABASE_FOR_SHARDING_RULES);
				targetTable = rule.getString(MyConstants.TARGET_TABLE_FOR_SHARDING_RULES);
				tempShardingRules.add(new ShardingRule(//
						Pattern.compile(regexDatabase), //
						Pattern.compile(regexTable), //
						targetDatabase, //
						targetTable));//
			}
		}
		// 开始构造对象并赋值
		attributes = new ConnectionAttributes(
				new SnapShot(null, SnapShotType.NONE, false, null, null, null, null, null, null));
		attributes.setUsernameForReplication(ur);
		attributes.setPasswordForReplication(pr);
		attributes.setUsernameForSchema(us);
		attributes.setPasswordForSchema(ps);
		// 设置filter规则
		attributes.setFilterRules(tempFilterRules);
		// 设置sharding规则
		attributes.setShardingRules(tempShardingRules);
		// 返回对象
		return attributes;

	}

	// 处理各种snapshot信息
	private SnapShot taskHeader = null;
	private SnapShot taskTailer = null;
	private Thread taskThread;

	public ConnectionAttributes(SnapShot shot) {
		// 刚开始初始化为一样的
		taskHeader = shot;
		taskTailer = shot;
	}

	public void addTaskToTail(SnapShot shot) {
		// 增加对全局唯一变量的有效性的检测
		if (false == shot.getGlobalValid().get()) {
			return;
		}
		taskTailer.setNext(shot);
		taskTailer = shot;
	}

	public Thread getTaskThread() {
		return taskThread;
	}

	public void startTaskThread() {
		Runnable runnable = new Runnable() {// 只属于观察者模式,不修改任何变量的值.ReadOnly
			// binlogzkpath从这个里面取
			String currentValues = null;
			int accumulatedIOEvent = 0;
			long begin = System.currentTimeMillis();

			@Override
			public void run() {
				// 条件性的无限循环中
				LoggerUtils.info(logger, "task Thread started...");
				//
				while (true == golbalValid.get()) {
					SnapShot shot = taskHeader.getNext();
					if (null == shot) {
                        TimeUtils.sleepMilliSeconds(10);// 主动休眠10ms
						continue;
					}
					// 拿到了一个有效的snapshot
					SnapShotType type = shot.getType();
					if (SnapShotType.ROTATE == type) {
						// 无条件更新
						LoggerUtils.info(logger, "Rotate event");
						ZooKeeperUtils.update(binlogPositionZKPath, shot.getZkValue());
						currentValues = null;
						accumulatedIOEvent = 0;
						begin = System.currentTimeMillis();
					} else if (SnapShotType.IO == type) {// IO类型

						while (true == golbalValid.get()
								&& WriteResultUtils.PENDING.ordinal() == shot.getWriteResult()) {
							// do nothing
							// 一直等待写入结果
							// 一切安好，否则退出
							TimeUtils.sleepMilliSeconds(1);
						}
						if (true == golbalValid.get() && WriteResultUtils.SUCCEED.ordinal() == shot.getWriteResult()) {
							// 确实写入成功
						} else {// 有情况,线程退出
							break;
						}

						// 考虑到按行加速,有可能是null
						// 只负责更新currentValues
						accumulatedIOEvent += (shot.getDatas().size() > 0 ? shot.getDatas().size() : 1);
						// LoggerUtils.info(logger,
						// "increase " + shot.getDatas().size() + " message,to
						// be " + accumulatedCount);
						if (null != shot.getZkValue()) {
							currentValues = shot.getZkValue();
						}

					} else if (SnapShotType.XID == type) {// xid类型
						// 只负责在超过阀值时，更新ZK,保证此位置肯定是一个XID的位置
						// LoggerUtils.info(logger, "XID event");
						if (null != currentValues
								&& accumulatedIOEvent >= MyProperties.getInstance().getAccumalatedCountValue()) {
							// 更新ZK
							if (begin > 0)
								LoggerUtils.info(logger, "update  binlog current position , cost xxx: "
										+ (System.currentTimeMillis() - begin) + " ms, count:" + accumulatedIOEvent
										+ " --- " + binlogPositionZKPath + ":" + currentValues);
							ZooKeeperUtils.update(binlogPositionZKPath, currentValues);
							currentValues = null;
							accumulatedIOEvent = 0;
							begin = System.currentTimeMillis();
						}
					} else {
						// what is wrong?
						LoggerUtils.info(logger, "unknown event type in ConnectionAttributes thread...");
					}
					// 更新taskHeader的位置
					taskHeader = shot;
				}
				//
				// 释放链接关系,促进垃圾回收
				taskHeader.setNext(null);
				LoggerUtils.info(logger, "task Thread exit...");
			}
		};
		taskThread = new Thread(runnable);
		taskThread.start();
	}

}