package com.hzw.monitor.mysqlbinlog.zookeeper;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.EnsurePath;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import com.hzw.monitor.mysqlbinlog.utils.LoggerUtils;

public class ZooKeeperUtils {
	
	private static final Logger logger = LogManager.getLogger(ZooKeeperUtils.class);
	// 需要记住PathChildrenCache
	// 一个唯一的路径，肯定对应着一个唯一的cache
	private static ConcurrentHashMap<String, PathChildrenCache> PathChildrenCaches = new ConcurrentHashMap<String, PathChildrenCache>();

	public static boolean registerChildListener(String path, PathChildrenCacheListener listener) {
		boolean result = false;
		try {
			PathChildrenCache cache = new PathChildrenCache(CuratorFrameworkClient.getInstance().getClient(), path,
					true);
			cache.start(StartMode.POST_INITIALIZED_EVENT);
			cache.getListenable().addListener(listener);
			result = true;
			// 保留
			PathChildrenCaches.put(path, cache);
		} catch (Exception e) {
			LoggerUtils.error(logger, e.toString());
		}
		return result;
	}

	public static boolean deRegisterChildListener(String path, PathChildrenCacheListener listener) {
		boolean result = false;
		try {
			PathChildrenCache cache = PathChildrenCaches.remove(path);
			if (null != cache) {
				cache.getListenable().removeListener(listener);
			}
			result = true;
		} catch (Exception e) {
			LoggerUtils.error(logger, e.toString());
		}
		return result;
	}

	// get children
	public static List<String> getChildren(String path) {
		try {
			return CuratorFrameworkClient.getInstance().getClient().getChildren().forPath(path);
		} catch (Exception e) {
			LoggerUtils.error(logger, e.toString());
		}
		return null;
	}

	// ensure class
	public static void ensurePersistentPathWithNoValue(String path) {
		try {
			EnsurePath ensurePath = new EnsurePath(path);
			ensurePath.ensure(CuratorFrameworkClient.getInstance().getClient().getZookeeperClient());
			LoggerUtils.debug(logger, "ensure zk path: " + path + " succeed");
			// LoggerUtils.debug(logger,
			// "--------------------------------------");
		} catch (Exception e) {

		}
	}

	// 是否存在
	public static boolean exist(String path) {
		CuratorFramework client = CuratorFrameworkClient.getInstance().getClient();
		boolean exist = false;
		try {
			if (null != client.checkExists().forPath(path)) {
				exist = true;
			}
		} catch (Exception e) {
			LoggerUtils.info(logger, e.toString());
		}
		return exist;
	}

	// 增加永久节点
	public static void createPersistent(String path, String value) {// 永久性的
		CuratorFramework client = CuratorFrameworkClient.getInstance().getClient();
		try {
			client.create().withMode(CreateMode.PERSISTENT).forPath(path, value.getBytes());
			// 创建永久路径
			LoggerUtils.debug(logger,
					"path not exist,create persistent path: " + path + " with value" + value + " succeed");
			// LoggerUtils.debug(logger,
			// "--------------------------------------");
		} catch (Exception e) {
			LoggerUtils.error(logger, e.toString());
		}
	}

	public static void upsertEphemeral(String path, String value) {
		if (ZooKeeperUtils.exist(path)) {
			ZooKeeperUtils.update(path, value);
		} else {
			ZooKeeperUtils.createEphemeral(path, value);
		}
	}

	// 增加临时节点
	public static void createEphemeral(String path, String value) {// 临时性的
		CuratorFramework client = CuratorFrameworkClient.getInstance().getClient();
		try {
			client.create().withMode(CreateMode.EPHEMERAL).forPath(path, value.getBytes());
			// 创建永久路径
			LoggerUtils.debug(logger, "path not exist,create temp path: " + path + " with value" + value + " succeed");
			// LoggerUtils.debug(logger,
			// "--------------------------------------");
		} catch (Exception e) {
			LoggerUtils.error(logger, e.toString());
		}
	}

	// 删
	public static void deletePath(String path) {
		try {
			CuratorFrameworkClient.getInstance().getClient().delete().guaranteed().forPath(path);
			LoggerUtils.debug(logger, "succeed to delete path:" + path);
		} catch (Exception e) {
			// LoggerUtils.debug(logger, e.toString());
		}
	}

	// 改
	public static void update(String path, String value) {
		long begin = System.currentTimeMillis();
		try {
			CuratorFrameworkClient.getInstance().getClient().setData().forPath(path, value.getBytes());
		} catch (Exception e) {
			// LoggerUtils.error(logger, e.toString());
		}
		long end = System.currentTimeMillis();
		LoggerUtils.debug(logger, "update zk data cost:" + (end - begin) + " ms " + path + ":" + value);
	}

	// 跟上面的代码完全一样，只不过为了区分是用在并行加速中的函数，没其它意思
	public static void parallelUpdate(String path, String value) {
		//long begin = System.currentTimeMillis();
		try {
			CuratorFrameworkClient.getInstance().getClient().setData().forPath(path, value.getBytes());
		} catch (Exception e) {
			LoggerUtils.error(logger, e.toString());
		}
		//long end = System.currentTimeMillis();
		// /LoggerUtils.info(logger, "update zk data cost:" + (end - begin) + "
		// ms");
	}

	// 查
	public static String getData(String path) {
		try {
			byte[] data = CuratorFrameworkClient.getInstance().getClient().getData().forPath(path);
			return new String(data);
		} catch (Exception e) {
			LoggerUtils.error(logger, e.toString());
		}
		return null;
	}

}
