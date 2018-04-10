package com.hzw.monitor.mysqlbinlog.mq;

import com.alibaba.fastjson.JSONObject;

public interface MQSender {

	public void send(String database, String table, JSONObject data);
}
