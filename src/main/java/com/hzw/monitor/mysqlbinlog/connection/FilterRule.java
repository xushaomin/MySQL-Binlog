package com.hzw.monitor.mysqlbinlog.connection;

import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;

public class FilterRule {
	private static final Logger logger = LogManager.getLogger(FilterRule.class);
	private Pattern database;
	private Pattern table;

	public FilterRule(Pattern d, Pattern t) {
		database = d;
		table = t;
	}

	public boolean accept(String d, String t) {
		// LoggerUtils.debug(logger, "database:" + database + " table:" +
		// table);
		LoggerUtils.debug(logger, "d:" + d + " t:" + t);
		//new Exception().printStackTrace();
		return database.matcher(d).matches() && table.matcher(t).matches();
	}

}