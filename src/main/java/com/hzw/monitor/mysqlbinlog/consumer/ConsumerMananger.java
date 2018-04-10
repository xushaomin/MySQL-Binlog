package com.hzw.monitor.mysqlbinlog.consumer;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import com.alibaba.fastjson.JSONObject;
import com.hzw.monitor.mysqlbinlog.connection.ConnectionAttributes;
import com.hzw.monitor.mysqlbinlog.snapshot.SnapShot;
import com.hzw.monitor.mysqlbinlog.type.ParallelType;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;
import com.hzw.monitor.mysqlbinlog.utils.MyProperties;
import com.hzw.monitor.mysqlbinlog.utils.StringUtils;
import com.hzw.monitor.mysqlbinlog.utils.TimeUtils;

public class ConsumerMananger {
	private static final Logger logger = LogManager.getLogger(ConsumerMananger.class);
	private Consumer[] consumers = null;
	private AtomicLong totalNumber = new AtomicLong(0);// 总的消费数量

	public void increase() {
		totalNumber.incrementAndGet();
	}

	public long getTotalNumber() {
		return totalNumber.get();
	}

	private ConsumerMananger(int count) {
		consumers = new Consumer[count];
		for (int index = 0; index < count; index++) {
			// 初始化+线程启动
			consumers[index] = new Consumer(index).start();
		}
	}

	public Consumer[] getConsumers() {
		return consumers;
	}

	public void addSnapShot(SnapShot shot, ConnectionAttributes myAttributes) {
		JSONObject jsonObject;
		ArrayList<String> pks;
		String database = shot.getDb();
		String table = shot.getTable();
		String unionStr = StringUtils.union(database, table);
		// 开始判断加速类型了
		ParallelType pType = shot.getParallelType();
		if (ParallelType.TABLE == pType) {
			// 什么都不做
		} else {// 按行复制,shot里面肯定只有一条数据
			jsonObject = shot.getDatas().get(0);
			pks = shot.getPks();
			for (String pk : pks) {// 因为主键不一定是string类型
				unionStr = StringUtils.union(unionStr, jsonObject.get(pk).toString());
			}
		}
		int hashIndex = unionStr.hashCode() % consumers.length;
		if (hashIndex < 0) {
			hashIndex += consumers.length;
		}
		// LoggerUtils.debug(logger, "db:table:" + database + ":" + table);
		// LoggerUtils.debug(logger,
		// "unionStr.hashCode():"+unionStr.hashCode());
		// LoggerUtils.debug(logger, "consumer index:"+hashIndex);
		Consumer consumer = consumers[hashIndex];
		consumer.addSnapShot(shot, myAttributes);
	}

	// 单例模式
	private static ConsumerMananger manager = null;

	static {
		manager = new ConsumerMananger(MyProperties.getInstance().getConsumer_Worker());
		new Thread(new Runnable() {
			// 打印剩余大小
			@Override
			public void run() {
				while (true) {
					// 每6秒打印1次
					TimeUtils.sleepMilliSeconds(6 * 1000);
					// 搜集
					String result = "[ totalItem-" + manager.getTotalNumber() + " ] [index - leftSize]";
					Consumer[] consumers = manager.getConsumers();
					for (Consumer consumer : consumers) {
						result += "[" + consumer.getIndex() + " - " + consumer.getQueueLeftSize() + "] ";
					}
					// 打印
					LoggerUtils.info(logger, result);
				}
			}
		}).start();
	}

	public static ConsumerMananger getInstance() {
		return manager;
	}

	public static synchronized void start() {
		// 触发static块被执行
	}

}
