package com.hzw.monitor.mysqlbinlog.utils;

import java.util.BitSet;
import java.util.HashMap;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 * @qq:837500869
 */
public class StringUtils {

	public static String union(String database, String table) {
		return database + "-" + table;
	}

	public static String[] map(BitSet bitset, HashMap<String, String> mappings) {
		// 截断收尾
		// LoggerUtils.debug(logger, "begin to map columns");
		String bitsetStr = bitset.toString();
		bitsetStr = bitsetStr.substring(1, bitsetStr.length() - 1);
		String[] positions = bitsetStr.split(",");
		// 开始构造
		// LoggerUtils.debug(logger, "positions length:" + positions.length);
		String[] columnNames = new String[positions.length];
		int index = 0;
		for (String position : positions) {
			position = position.trim();
			// LoggerUtils.debug(logger, position + "-" + column);
			columnNames[index++] = mappings.get(position);
		}
		return columnNames;
	}

}
