package com.hzw.monitor.mysqlbinlog.utils;

import java.util.ArrayList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.hzw.monitor.mysqlbinlog.consumer.ConsumerMananger;

public class DataUtils {
	
	private static final Logger logger = LogManager.getLogger(DataUtils.class);
	
	public static boolean handle(String database, String table, JSONObject data) {
		// 默认为false
		boolean result = false;
		// 所以，存储的结果一定要赋值给result
		// !!!!!!!!!!!!!用户自定义业务逻辑开始
		result = MQUtils.sendMqQueue(database, table, data);
		// TimeUtils.sleepMilliSeconds(6);// 模拟业务耗时6ms
		// logger.info("-----------------");
		// logger.info("[" + database + "] [" + table + "] --- " + data.toJSONString());
		// logger.info("-----------------");
		result = true;
		// !!!!!!!!!!!!!用户自定义业务逻辑结束
		return result;
	}

	public static boolean handleMulti(String database, String table, ArrayList<JSONObject> datas) {
		if (null == datas || datas.size() == 0) {
			return true;
		}
		// 可以开始处理每一条了
		// 只要其中一个发生错误，就立刻返回,剩下的也不用处理了
		for (JSONObject data : datas) {
			if (null == data) {
				continue;
			}
			if (false == DataUtils.handle(database, table, data)) {
				LoggerUtils.error(logger, "handleMulti ,but error happened");
				return false;
			}
			ConsumerMananger.getInstance().increase();			
			// LoggerUtils.debug(logger, data.toJSONString());
		}
		// 返回结果
		return true;
	}
}
