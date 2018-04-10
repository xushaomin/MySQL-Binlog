package com.hzw.monitor.mysqlbinlog.utils;

import com.alibaba.fastjson.JSONObject;
import com.hzw.monitor.mysqlbinlog.mq.MQSender;

public class MQUtils {

	private static String MQ_SENDER_CLASS;

	private static MQSender mqSender;

	public static boolean sendMqQueue(String database, String table, JSONObject data) {
		getInstant().send(database, table, data);
		return true;
	}

	private static MQSender getInstant() {
		if (null != mqSender) {
			return mqSender;
		} else {
			try {
				Class<?> clazz = Class.forName(MQ_SENDER_CLASS);
				mqSender = (MQSender) clazz.newInstance();
			} catch (Exception e) {
				return null;
			}
			return mqSender;
		}
	}
	
	private static void instant() {
		try {
			Class<?> clazz = Class.forName(MQ_SENDER_CLASS);
			mqSender = (MQSender) clazz.newInstance();
		} catch (Exception e) {
		}

	}

	public static void setSenderClass(String mqSenderClass) {
		MQUtils.MQ_SENDER_CLASS = mqSenderClass;
		instant();
	}

	public static void setSenderClass(Class<?> clazz) {
		try {
			mqSender = (MQSender) clazz.newInstance();
		} catch (Exception e) {
		}
	}
	
	public static void setMqSender(MQSender mqSender) {
		MQUtils.mqSender = mqSender;
	}

}
