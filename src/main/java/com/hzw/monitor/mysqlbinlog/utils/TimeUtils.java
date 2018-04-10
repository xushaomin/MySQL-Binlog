package com.hzw.monitor.mysqlbinlog.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
public class TimeUtils {
	public static void sleepSeconds(long t) {// 睡眠相关秒
		sleepMilliSeconds(t * 1000);
	}

	@SuppressWarnings("static-access")
	public static void sleepMilliSeconds(long t) {// 睡眠相关毫秒
		try {
			Thread.currentThread().sleep(t);
		} catch (Exception e) {
		}
	}

	private static final ThreadLocal<SimpleDateFormat> dayDateFormat = new ThreadLocal<SimpleDateFormat>() {
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyyMMdd");
		}
	};

	// 获得当前天
	public static String getTimeDay(long time) {
		return dayDateFormat.get().format(new Date(time));
	}

	// 获得当天时间戳
	public static long getDayTime(String day) throws ParseException {
		return dayDateFormat.get().parse(day).getTime();
	}

	public static void main(String[] args) {
		// long t = 1460719581000;
		// String day = TimeUtils.getTimeDay(t);
	}

}
