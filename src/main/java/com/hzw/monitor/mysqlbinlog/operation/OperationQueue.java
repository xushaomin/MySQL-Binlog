package com.hzw.monitor.mysqlbinlog.operation;

/**
 * 
 * @author zhiqiang.liu
 * @2016年1月1日
 *
 */
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;

/**
 * @author zhiqiang.liu 2015年11月13日
 * @QQ:837500869
 */
public class OperationQueue {// 专门存储各种事件
	// poll: 若队列为空，返回null。
	// remove:若队列为空，抛出NoSuchElementException异常。
	// take:若队列为空，发生阻塞，等待有元素。

	// put---无空间会等待
	// add--- 满时,立即返回,会抛出异常
	// offer---满时,立即返回,不抛异常

	private static final Logger logger = LogManager.getLogger(OperationQueue.class);
	public static BlockingQueue<OpeationEvent> objectQueue = new LinkedBlockingQueue<OpeationEvent>();

	public static void addObject(OpeationEvent obj) {
		try {
			objectQueue.put(obj);
		} catch (InterruptedException e) {
			LoggerUtils.error(logger, "fail to put event into operation queue" + e.toString());
		}
	};

	public static OpeationEvent getObject() {
		return objectQueue.poll();
	}

}