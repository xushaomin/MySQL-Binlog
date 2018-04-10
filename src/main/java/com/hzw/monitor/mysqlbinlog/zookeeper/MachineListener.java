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

public class MachineListener {
	private PathChildrenCacheListener listener;
	private static final Logger logger = LogManager.getLogger(MachineListener.class);

	private MachineListener(PathChildrenCacheListener l) {
		listener = l;
	}

	public PathChildrenCacheListener getListener() {
		return listener;
	}

	///
	private static MachineListener instance;

	public static MachineListener getInstance() {
		if (null == instance) {
			synchronized (MachineListener.class) {
				if (null == instance) {
					PathChildrenCacheListener p = new PathChildrenCacheListener() {
						@Override
						public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
							// 正式处理业务
							switch (event.getType()) {
							case CHILD_ADDED:
								// 放入队列就闪
								LoggerUtils.debug(logger, "machines added...");
								OperationQueue.addObject(new OpeationEvent(event, OperationType.MACHINE_ADD));
								break;
							case CHILD_UPDATED:
								// 放入队列就闪
								LoggerUtils.debug(logger, "machines updated...");
								OperationQueue.addObject(new OpeationEvent(event, OperationType.MACHINE_UPDATE));
								break;
							case CHILD_REMOVED:
								// 放入队列就闪
								LoggerUtils.debug(logger, "machines deleted...");
								OperationQueue.addObject(new OpeationEvent(event, OperationType.MACHINE_DELETE));
								break;
							default:
								break;
							}
						}
					};
					instance = new MachineListener(p);
				}
			}
		}
		return instance;
	}
}
