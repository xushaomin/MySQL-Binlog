package com.hzw.monitor.mysqlbinlog.zookeeper;

import org.apache.curator.RetryPolicy;
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

	private static CuratorFrameworkClient instance;

	public static CuratorFrameworkClient getInstance() {
		if (null == instance) {
			synchronized (CuratorFrameworkClient.class) {
				if (null == instance) {
					MyProperties prop = MyProperties.getInstance();
			        RetryPolicy retryPolicy = 
			        		new ExponentialBackoffRetry(prop.getZk_retry_time(), prop.getZk_retry_max());
					CuratorFramework worker = CuratorFrameworkFactory.builder()
							.connectString(prop.getZk_servers())
							.sessionTimeoutMs(prop.getZk_session_timeout())
							.retryPolicy(retryPolicy).build();
					worker.start();
					worker.usingNamespace(MyConstants.ZK_NAMESPACE);
					instance = new CuratorFrameworkClient(worker);
				}
			}
		}
		return instance;
	}
}
