package com.hzw.monitor.mysqlbinlog.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.hzw.monitor.mysqlbinlog.utils.MyConstants;
import com.hzw.monitor.mysqlbinlog.utils.MyProperties;

public class CuratorFrameworkClient {
	private CuratorFramework client;

	private CuratorFrameworkClient(CuratorFramework c) {
		client = c;
	}

	public CuratorFramework getClient() {
		return client;
	}

	///
	///
	///
	private static CuratorFrameworkClient instance;

	public static CuratorFrameworkClient getInstance() {
		if (null == instance) {
			synchronized (CuratorFrameworkClient.class) {
				if (null == instance) {
					CuratorFramework worker = CuratorFrameworkFactory.builder()
							.connectString(MyProperties.getInstance().getZk_servers()).sessionTimeoutMs(5000)
							.retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
					worker.start();
					worker.usingNamespace(MyConstants.ZK_NAMESPACE);
					instance = new CuratorFrameworkClient(worker);
				}
			}
		}
		return instance;
	}
}
