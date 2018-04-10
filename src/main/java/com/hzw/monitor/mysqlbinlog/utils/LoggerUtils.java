package com.hzw.monitor.mysqlbinlog.utils;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
import org.apache.log4j.Logger;

public class LoggerUtils {// log4j
	public static synchronized void debug(Logger logger, String msg) {
		if (logger.isDebugEnabled()) {
			logger.debug(msg);
		}
	}

	public static void info(Logger logger, String msg) {
		if (logger.isInfoEnabled()) {
			logger.info(msg);
		}

	}

	public static void error(Logger logger, String msg) {
		// 错误直接打
		logger.error(msg);
	}
}
