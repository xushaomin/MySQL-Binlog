package com.hzw.monitor.mysqlbinlog.consumer;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.hzw.monitor.mysqlbinlog.connection.ConnectionAttributes;
import com.hzw.monitor.mysqlbinlog.connection.ShardingRule;
import com.hzw.monitor.mysqlbinlog.snapshot.SnapShot;
import com.hzw.monitor.mysqlbinlog.utils.DataUtils;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;
import com.hzw.monitor.mysqlbinlog.utils.MyConstants;
import com.hzw.monitor.mysqlbinlog.utils.TimeUtils;
import com.hzw.monitor.mysqlbinlog.utils.WriteResultUtils;

public class Consumer {

	private static final Logger logger = LogManager.getLogger(Consumer.class);
	
	private int index;
	private BlockingQueue<SnapShot> queue = null;
	private Thread thread = null;

	public Consumer(int id) {
		index = id;
		queue = new LinkedBlockingQueue<SnapShot>(1 * 10000);// 1万条
	}

	public int getIndex() {
		return index;
	}

	public long getQueueLeftSize() {
		return queue.remainingCapacity();
	}

	// poll: 若队列为空，返回null。
	// remove:若队列为空，抛出NoSuchElementException异常。
	// take:若队列为空，发生阻塞，等待有元素。
	public Consumer start() {
		// 1)
		Runnable runnable = new Runnable() {
			@Override
			public void run() {
				// 开始处理队列里的每一个数据
				SnapShot shot = null;
				while (true) {// 无限循环中,本线程永远不会退出
					// LoggerUtils.info(logger, "queue("+index+").size: "+queue.size());
					try {// 尝试拿取数据
						shot = queue.take();
					} catch (InterruptedException e) {
						shot = null;
					}
					// 为空则继续
					if (null == shot) {
						LoggerUtils.info(logger, "no message from consume queue");
                            TimeUtils.sleepMilliSeconds(10);// 主动休眠10ms
						continue;
					}
					// 成功拿到了一个SnapShot对象
					// 1)检测是否成功标志
					if (false == shot.getGlobalValid().get()) {
						shot.setWriteResult(WriteResultUtils.FAIL.ordinal());
						continue;
					}
					// 当前有效
					// type肯定是IO类型，否则不会加入到这里
					int result;
					if (shot.isAccepted()) {// 确实需要处理
						result = DataUtils.handleMulti(shot.getDb(), shot.getTable(), shot.getDatas())
								? WriteResultUtils.SUCCEED.ordinal() : WriteResultUtils.FAIL.ordinal();
					} else {
						result = WriteResultUtils.SUCCEED.ordinal();
					}
					// 将结果写回到result
					shot.setWriteResult(result);
					// 如果写入失败，设置全局失败标志
					if (WriteResultUtils.FAIL.ordinal() == result) {
						shot.getGlobalValid().set(false);
					}
				}
			}
		};
		// 2)
		thread = new Thread(runnable);
		// 3)
		thread.setDaemon(true);// 后台线程
		thread.setName("consumer_thread_" + index);
		thread.start();
		LoggerUtils.info(logger, thread.getName() + " started successfully...");
		return this;

	}

	// put---无空间会等待
	// add--- 满时,立即返回,会抛出异常
	// offer---满时,立即返回,不抛异常
	public void addSnapShot(SnapShot snapShot, ConnectionAttributes myAttributes) {
		if (null == snapShot) {
			return;
		}
		// 增加分库分表的改写规则,这里是最后的机会改写
		// 放在最后，也是为了防止破坏之前已经稳定的业务逻辑，避免引入新的bug
		if (snapShot.isAccepted()) {// 如果是需要丢弃的数据，就不要浪费时间了
			ShardingRule rule = myAttributes.findShardingRule(snapShot.getDb(), snapShot.getTable());
			if (null != rule) {// 确实命中了一个分库分表规则
				updateSnapShotSelf(rule.getTargetDatabase(), rule.getTargetTable(), snapShot);
				updateSnapShotData(rule.getTargetDatabase(), rule.getTargetTable(), snapShot.getDatas());
			}
		}
		//
		// 坚决不能阻塞，否则selector会挂住
		while (true == snapShot.getGlobalValid().get()) {
			// 如果还是有效的，则需要进入
			try {
				queue.add(snapShot);
				break;
			} catch (Exception e) {
				continue;
			}
		}
	}

	private void updateSnapShotSelf(String db, String table, SnapShot shot) {
		shot.setDb(db);
		shot.setTable(table);
	}

	//
	private void updateSnapShotData(String db, String table, ArrayList<JSONObject> datas) {
		if (null == datas) {
			return;
		}
		// 遍历
		// {"optType":"UPDATE","databaseName":"skyeye","id":2,"optTime":1461740209000,"tableName":"aaa"}
		for (JSONObject obj : datas) {
			obj.put(MyConstants.DATABASE, db);
			obj.put(MyConstants.TABLE, table);
		}
	}

}