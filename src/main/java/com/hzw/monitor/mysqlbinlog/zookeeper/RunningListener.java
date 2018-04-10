package com.hzw.monitor.mysqlbinlog.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hzw.monitor.mysqlbinlog.operation.OpeationEvent;
import com.hzw.monitor.mysqlbinlog.operation.OperationQueue;
import com.hzw.monitor.mysqlbinlog.operation.OperationType;
import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;

public class RunningListener {
	private PathChildrenCacheListener listener;

	private RunningListener(PathChildrenCacheListener l) {
		listener = l;
	}

	public PathChildrenCacheListener getListener() {
		return listener;
	}
	///

	private static RunningListener instance;
	private static final Logger logger = LogManager.getLogger(RunningListener.class);

	public static RunningListener getInstance() {
		if (null == instance) {
			synchronized (RunningListener.class) {
				if (null == instance) {
					PathChildrenCacheListener listener = new PathChildrenCacheListener() {
						@Override
						public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
							// 正式处理业务
							switch (event.getType()) {
							case CHILD_REMOVED:
								String currentPath = event.getData().getPath();
								LoggerUtils.debug(logger, this + "running/current removed," + currentPath);
								OpeationEvent operationEvent = new OpeationEvent(event, OperationType.RUNNING_DELETE);
								OperationQueue.addObject(operationEvent);
								break;
							default:
								break;
							}
						}
					};

					instance = new RunningListener(listener);
				}
			}
		}
		return instance;
	}
}
