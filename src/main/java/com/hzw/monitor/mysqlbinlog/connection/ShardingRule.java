package com.hzw.monitor.mysqlbinlog.connection;

import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;

public class ShardingRule {
	// 支持分库分表
	private static final Logger logger = LogManager.getLogger(ShardingRule.class);
	// 私有变量
	private Pattern originDatabase;
	private Pattern originTable;
	// 之后的
	private String targetDatabase;
	private String targetTable;

	// 构造函数
	public ShardingRule(Pattern oDatabase, Pattern oTable, String tDatabase, String tTable) {
		originDatabase = oDatabase;
		originTable = oTable;
		targetDatabase = tDatabase;
		targetTable = tTable;
		LoggerUtils.info(logger, "regex sharding rule : [" + originDatabase + "],[" + originTable + "],["
				+ targetDatabase + "],[" + targetTable + "]");
	}

	public boolean accept(String d, String t) {
		// LoggerUtils.debug(logger, "database:" + database + " table:" +
		// table);
		LoggerUtils.debug(logger, "d:" + d + " t:" + t);
		// new Exception().printStackTrace();
		return originDatabase.matcher(d).matches() && originTable.matcher(t).matches();
	}

	public String getTargetDatabase() {
		return targetDatabase;
	}

	public String getTargetTable() {
		return targetTable;
	}
}
