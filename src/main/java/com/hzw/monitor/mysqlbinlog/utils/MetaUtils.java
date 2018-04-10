package com.hzw.monitor.mysqlbinlog.utils;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MetaUtils {

	private static final Logger logger = LogManager.getLogger(MetaUtils.class);
	// 4个字段
	private static final String TABLE_SCHEMA = "TABLE_SCHEMA";
	private static final String TABLE_NAME = "TABLE_NAME";
	private static final String COLUMN_NAME = "COLUMN_NAME";
	private static final String ORDINAL_POSITION = "ORDINAL_POSITION";

	//
	public static HashMap<String, HashMap<String, String>> getAllMetaInformationsPerConnection(//
			String ips, int port, String username, String password) throws SQLException {
		// 启动一个任务时，一次性全量拉取meta信息
		// 开始拉取
		java.sql.Connection conn = null;
		Statement stmt = null;
		ResultSet resultSet = null;
		String[] ipArray = ips.split(",");// 一个一个尝试
		for (String ip : ipArray) {
			HashMap<String, HashMap<String, String>> mapping = new HashMap<String, HashMap<String, String>>();
			//
			try {
				// 3) sql
				String sql = "select TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME, ORDINAL_POSITION ";
				sql += " from INFORMATION_SCHEMA.COLUMNS ";
				sql += " order by TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION";
				// 4)创建连接
				String url = "jdbc:mysql://" + ip + ":" + port + "/";
				conn = DriverManager.getConnection(url, username, password);
				stmt = conn.createStatement();
				resultSet = stmt.executeQuery(sql);
				// 开始获取结果
				String database;
				String table;
				String columnName;
				int position;
				String key;

				while (null != resultSet && resultSet.next()) {// 为了简单处理，这里就不做过分优化了,一次性的
					// 针对每一行进行处理
					database = resultSet.getString(TABLE_SCHEMA);
					table = resultSet.getString(TABLE_NAME);
					columnName = resultSet.getString(COLUMN_NAME);
					position = resultSet.getInt(ORDINAL_POSITION);
					// 先取到指定的哈希表
					key = StringUtils.union(database, table);
					HashMap<String, String> subMapping = mapping.get(key);
					if (null == subMapping) {
						subMapping = new HashMap<String, String>();
						mapping.put(key, subMapping);
					}
					// 加入到指定的哈希表里
					subMapping.put("" + (position - 1), columnName);
				}
			} catch (Exception e) {
				LoggerUtils.error(logger, e.toString());
				mapping = null;
			} // 获取连接
			finally {
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

			if (null != mapping) {
				// 当前的IP获取成功
				return mapping;
			}
		}
		// 没办法
		return null;
	}

	public static HashMap<String, ArrayList<String>> getAllPimaryKeyInformationsPerConnection(//
			String ips, int port, String username, String password) {
		// 启动一个任务时，一次性全量拉取meta信息
		//
		// 开始拉取
		java.sql.Connection conn = null;
		Statement stmt = null;
		ResultSet resultSet = null;
		String[] ipArray = ips.split(",");// 多个IP,一个一个尝试
		for (String ip : ipArray) {
			HashMap<String, ArrayList<String>> mapping = new HashMap<String, ArrayList<String>>();
			try {
				// 3) sql
				String sql = " SELECT t.table_schema,t.table_name ,k.column_name  ";
				sql += " FROM information_schema.table_constraints t ";
				sql += " JOIN information_schema.key_column_usage k ";
				sql += " USING (constraint_name,table_schema,table_name) ";
				sql += " WHERE t.constraint_type='PRIMARY KEY' ";
				sql += " order by table_schema,table_name,column_name";
				// 4)创建连接
				String url = "jdbc:mysql://" + ip + ":" + port + "/";
				conn = DriverManager.getConnection(url, username, password);
				stmt = conn.createStatement();
				resultSet = stmt.executeQuery(sql);
				// 开始获取结果
				String database;
				String table;
				String columnName;
				String key;

				while (null != resultSet && resultSet.next()) {// 为了简单处理，这里就不做过分优化了,一次性的
					// 针对每一行进行处理
					database = resultSet.getString("table_schema");
					table = resultSet.getString("table_name");
					columnName = resultSet.getString("column_name");
					// 先取到指定的哈希表
					key = StringUtils.union(database, table);
					ArrayList<String> subList = mapping.get(key);
					if (null == subList) {
						subList = new ArrayList<String>();
						mapping.put(key, subList);
					}
					// 加入到指定的list里
					subList.add(columnName);
				}
			} catch (Exception e) {
				LoggerUtils.error(logger, e.toString());
				mapping = null;
			} // 获取连接
			finally {
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
			// 判断本次获取是否成功
			if (null != mapping) {
				return mapping;
			}
		} // for循环结束
		return null;
	}
}
