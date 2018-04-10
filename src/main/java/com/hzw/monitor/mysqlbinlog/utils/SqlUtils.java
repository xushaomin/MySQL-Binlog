package com.hzw.monitor.mysqlbinlog.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TTable;

public class SqlUtils {
	
	private static final Logger logger = LogManager.getLogger(SqlUtils.class);

	//private static final String TABLE_SCHEMA = "TABLE_SCHEMA";
	//private static final String TABLE_NAME = "TABLE_NAME";
	private static final String COLUMN_NAME = "COLUMN_NAME";
	private static final String ORDINAL_POSITION = "ORDINAL_POSITION";

	private static void getPrimaryKeyMapping(Connection conn, String database, String table,
			HashMap<String, ArrayList<String>> primaryKeysMapping) throws Exception {
		// 初始化为空，表示不存在主键
		ArrayList<String> pks = new ArrayList<String>();
		// 准备sql语句
		String sql = " SELECT k.column_name ";
		sql += " FROM information_schema.table_constraints t ";
		sql += " JOIN information_schema.key_column_usage k ";
		sql += " USING (constraint_name,table_schema,table_name) ";
		sql += " WHERE t.constraint_type='PRIMARY KEY' ";
		sql += " AND t.table_schema='" + database + "' ";
		sql += " AND t.table_name='" + table + "' ";
		sql += " order by k.column_name";
		// 开始查询
		Statement stmt = null;
		ResultSet resultSet = null;
		String columnName;
		try {
			stmt = conn.createStatement();
			resultSet = stmt.executeQuery(sql);
			while (null != resultSet && resultSet.next()) {
				columnName = resultSet.getString(COLUMN_NAME);
				pks.add(columnName);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			// 必须分开关闭
			// 关闭resultset
			if (null != resultSet) {
				try {
					resultSet.close();
				} catch (SQLException e) {
				}
			}
			// 关闭stmt
			if (null != stmt) {
				try {
					stmt.close();
				} catch (SQLException e) {
				}
			}
		}
		// LoggerUtils.debug(logger, "pks:[" + database + "][" + table + "] ---"
		// + pks);
		primaryKeysMapping.put(StringUtils.union(database, table), pks);// 更新此数据
		// 返回
	}

	public static HashMap<String, String> getDatabaseTableColumnsMapping(String ip, int port, String username,
			String password, String database, String table, HashMap<String, ArrayList<String>> primaryKeysMapping)
			throws Exception {
		// 通过查询数据库来获取消息
		// LoggerUtils.debug(logger, "-------execute
		// getDatabaseTableColumnsMapping ... pity...because database-table ["
		// + database + "-" + table + " ] structure is changed...");
		HashMap<String, String> mappings = new HashMap<String, String>();
		Connection conn = null;
		Statement stmt = null;
		ResultSet resultSet = null;
		try {
			String url = "jdbc:mysql://" + ip + ":" + port + "/" + database;
			conn = DriverManager.getConnection(url, username, password);// 获取连接
			stmt = conn.createStatement();
			// 构造sql
			String sql = "select COLUMN_NAME, ORDINAL_POSITION ";
			sql += " from INFORMATION_SCHEMA.COLUMNS ";
			sql += "where TABLE_SCHEMA='" + database + "' and TABLE_NAME='" + table + "'";
			sql += " order by ORDINAL_POSITION";
			// LoggerUtils.debug(logger, "sql:" + sql);
			resultSet = stmt.executeQuery(sql);
			String columnName;
			int position;
			while (null != resultSet && resultSet.next()) {
				// 存在下一行数据
				// String tableSchema = resultSet.getString(TABLE_SCHEMA);
				// String tableSchema = resultSet.getString(TABLE_SCHEMA);
				columnName = resultSet.getString(COLUMN_NAME);
				position = resultSet.getInt(ORDINAL_POSITION);
				// LoggerUtils.debug(logger, "" + columnName + " " + position);
				mappings.put("" + (position - 1), columnName);
			}
			// 借着这个connection,获取主键关系
			getPrimaryKeyMapping(conn, database, table, primaryKeysMapping);
			LoggerUtils.debug(logger, "connect to mysql to get meta mapping succeed..." + ip + ":" + port);
		} catch (Exception e) {
			LoggerUtils.error(logger,
					"connect to mysql to get meta mapping fail..." + ip + ":" + port + " " + e.toString());
			throw e;
		} finally {
			// 必须分开关闭
			// 关闭resultset
			if (null != resultSet) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			// 关闭stmt
			if (null != stmt) {
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			// 关闭conn
			if (null != conn) {
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		// LoggerUtils.debug(logger, "-----------------------------query mapping
		// end");
		return mappings;
	}

	public static boolean isAlterTableSql(String sql) {
		sql = sql.toUpperCase();
		return sql.startsWith("ALTER TABLE");
	}

	public static boolean isDropTableSql(String sql) {
		sql = sql.toUpperCase();
		return sql.startsWith("DROP TABLE");
	}

	// 1)JSqlParser
	// http://search.maven.org/#search%7Cga%7C1%7Cjsqlparser
	// https://sourceforge.net/projects/jsqlparser/files/ [old]
	// https://github.com/JSQLParser/JSqlParser
	// https://github.com/JSQLParser/JSqlParser/wiki
	//
	//
	//
	private static final ThreadLocal<TGSqlParser> localTGSqlParsers = new ThreadLocal<TGSqlParser>() {
		protected TGSqlParser initialValue() {
			return new TGSqlParser(EDbVendor.dbvmysql);
		}
	};
	// 2)SqlParser
	// http://www.sqlparser.com/
	// https://github.com/sushilshah/gsp-sqlparser
	// https://github.com/sushilshah/gsp-sqlparser/blob/master/demos/getstatement/getstatement.java

	public static String getTableNameBySqlParser(String sql) {
		// LoggerUtils.debug(logger, "---------------------------------------");
		String result = null;
		// 1)准备工作
		TGSqlParser sqlParser = localTGSqlParsers.get();
		sqlParser.sqltext = sql;
		TTable table;
		// 2)准备解析
		if (0 == sqlParser.parse() && sqlParser.sqlstatements.size() > 0) {
			// succeed
			// gudusoft.gsqlparser.ESqlStatementType sqlstatementtype;
			// LoggerUtils.debug(logger, "parse sql succeed");
			TCustomSqlStatement stmt = sqlParser.sqlstatements.get(0);
			switch (stmt.sqlstatementtype) {
			case sstaltertable:
			case sstdroptable:
				table = stmt.getTargetTable(); //.getFirstPhysicalTable();
				if (null != table) {
					result = table.getName();
				}
				break;
			default:// 其它的暂时不支持,需要的话再扩展
				break;
			}
		} else {
			LoggerUtils.debug(logger, "fail to parse " + sql + ",will try another method");
		}

		// 3)如果为null,做一些补救措施
		if (null == result) {
			String[] strArray = sql.split(" ");
			if (null != strArray && strArray.length >= 3) {
				result = strArray[2];
				// printString(result);
				// 可能存在.字符
				int index = result.indexOf(pointStr);
				if (-1 != index) {
					result = result.substring(index + 1);
				}
			}
		}

		// 4)规整化,可能有\0X60字符
		if (null != result) {
			LoggerUtils.debug(logger, "succeed to get table name...");
			// System.out.println(result.getBytes()[0]);
			if (0 == result.indexOf(keyStr)) {
				result = result.substring(1);
			}
			if (result.length() - 1 == result.indexOf(keyStr)) {
				result = result.substring(0, result.length() - 1);
			}
		}

		return result;
	}

	@SuppressWarnings("unused")
	private static void printString(String str) {
		byte[] bytes = str.getBytes();
		for (int index = 0; index < bytes.length; index++) {
			System.out.println(bytes[index]);
		}
	}

	public static void main(String[] args) {
		String sql = "drop table tab1 ";
		System.out.println(getTableNameBySqlParser(sql));
		sql = "drop table `tab1` ";
		System.out.println(getTableNameBySqlParser(sql));
		sql = "alter table http_monitor add column statistic char(1) DEFAULT NULL COMMENT '234'";
		System.out.println(getTableNameBySqlParser(sql));
		sql = "Alter table `skyeye`.`servicekey_product`     drop column `subkey`";
		System.out.println(getTableNameBySqlParser(sql));
		sql = "Alter table `servicekey_product`     drop column `subkey`";
		System.out.println(getTableNameBySqlParser(sql));
		sql = "Alter table `skyeye`.`risk_threshold_monitor`    change `pcode` `product_code` char(50) CHARSET utf8 COLLATE utf8_general_ci NOT NULL";
		System.out.println(getTableNameBySqlParser(sql));
	}

	public synchronized static void init() {
		// 触发static块
	}

	private static final String name = "com.mysql.jdbc.Driver";
	private static String keyStr = null;
	private static String pointStr = null;
	static {
		try {
			Class.forName(name);
		} catch (ClassNotFoundException e) {
			LoggerUtils.error(logger, e.toString());
			System.exit(-1);
		}
		//
		byte[] key = new byte[1];
		key[0] = 0x60;
		keyStr = new String(key);
		key = new byte[1];
		key[0] = 0x2E;
		pointStr = new String(key);
	}
}
