package jailer.jdbc;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import jailer.core.CommonUtil;
import jailer.core.PathManager;
import jailer.core.ZookeeperTimeOutConf;
import jailer.core.encrypt.JailerAESEncryption;
import jailer.core.encrypt.JailerEncryption;
import jailer.core.model.ConnectionInfo;
import jailer.core.model.ConnectionKey;
import jailer.core.model.DataSourceKey;
import jailer.core.model.JailerDataSource;
import jailer.jdbc.model.ConnectionKeyData;

public class JdbcRepositoryCurator {
	private Logger log = Logger.getLogger(JdbcRepositoryCurator.class);
	
	private final CuratorFramework client;
	private final JailerEncryption encryption = new JailerAESEncryption();

	// Timeout
	private static final int default_sessionTimeoutMs = 20 * 1000;
	private static final int default_connectionTimeoutMs = 10 * 1000;
	
	// ExponentialBackoffRetry
	private static final int default_baseSleepTimeMs = 1000;
	private static final int default_maxRetries = 3;
	
	// 再Watch用Map
	private Map<ConnectionKey, CuratorWatcher> SessionExpiredWatcherMap = new ConcurrentHashMap<>();
	
	// connectionノード再生成用
	private Map<ConnectionKey, ConnectionInfo> connectionKeyMap = new ConcurrentHashMap<>();
	
//	public JdbcRepositoryCurator(String connectString){
//		this(connectString, getDefaultRetryPolicy(), getDefaultZookeeperTimeOutConf());
//	}
	
//	private static ZookeeperTimeOutConf getDefaultZookeeperTimeOutConf() {
//		return new ZookeeperTimeOutConf(default_sessionTimeoutMs, default_connectionTimeoutMs);
//	}
//
//	private static RetryPolicy getDefaultRetryPolicy() {
//		return new ExponentialBackoffRetry(default_baseSleepTimeMs, default_maxRetries);
//	}
	
	public JdbcRepositoryCurator(CuratorFramework client){
		this.client = client;
		this.client.getCuratorListenable().addListener(new DefaultListener());
		this.client.getConnectionStateListenable().addListener(new ReConnectedListener());
	}

//	public JdbcRepositoryCurator(String connectString, RetryPolicy retryPolicy, ZookeeperTimeOutConf conf){
//		this.client = CuratorFrameworkFactory.builder().
//        connectString(connectString).
//        sessionTimeoutMs(conf.getSessionTimeoutMs()).
//        connectionTimeoutMs(conf.getConnectionTimeoutMs()).
//        retryPolicy(retryPolicy).
//        build();
//		this.client.start();
//		this.client.getCuratorListenable().addListener(new DefaultListener());
//		this.client.getConnectionStateListenable().addListener(new ReConnectedListener());
//	}
	
	public DataSourceKey getDataSourceKey(String uuid) throws Exception{
		byte[] result = client.getData().forPath(PathManager.getUuidPath(uuid));
		log.trace("getDataSourceKey() path : " + PathManager.getUuidPath(uuid));
		return CommonUtil.jsonToObject(encryption.decrypt(result), DataSourceKey.class);
	}

	public JailerDataSource getJailerDataSource(DataSourceKey key) throws Exception{
		byte[] result = client.getData().forPath(PathManager.getDataSourceCorrentPath(key));
		log.trace("getJailerDataSource() path : " + PathManager.getDataSourceCorrentPath(key));
		log.trace("getJailerDataSource() result : " + encryption.decrypt(result));
		return CommonUtil.jsonToObject(encryption.decrypt(result), JailerDataSource.class);
	}
	
	public void watchDataSource(ConnectionKey key, CuratorWatcher watcher) throws Exception{
		client.checkExists().usingWatcher(watcher).forPath(PathManager.getDataSourceCorrentPath(key));
		SessionExpiredWatcherMap.put(key, watcher);
		log.trace("SessionExpiredWatcherMap put : " + key);
	}
	
	public ConnectionKeyData registConnection(DataSourceKey key, ConnectionInfo info) throws Exception{
		String data = CommonUtil.objectToJson(info);
		String connectionPath = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(PathManager.getDataSourceCorrentPath(key) + "/", encryption.encrypt(data));
		
		ConnectionKeyData connectionData = new ConnectionKeyData();
		connectionData.setServiceId(key.getServiceId());
		connectionData.setGroupId(key.getGroupId());
		connectionData.setDataSourceId(key.getDataSourceId());
		connectionData.setConnectionId(connectionPath.substring(connectionPath.length() - 10, connectionPath.length()));
		connectionData.setInfo(info);
		
		connectionKeyMap.put(connectionData, info);
		log.trace("connectionKeyMap put : " + connectionData);

		log.info("createConnection : " + connectionData);
		
		return connectionData;
	}
	
	public void repairConnectionNode(ConnectionKey key, ConnectionInfo info) throws Exception{
		String data = CommonUtil.objectToJson(info);
		if(isExistsConnectionNode(key)){
			client.delete().forPath(PathManager.getConnectionPath(key));
		}
		client.create().withMode(CreateMode.EPHEMERAL).forPath(PathManager.getConnectionPath(key), encryption.encrypt(data));
	}
	
	private boolean isExistsConnectionNode(ConnectionKey key) throws Exception{
		Stat stat = client.checkExists().forPath(PathManager.getConnectionPath(key));
		
		if(stat != null){
			return true;
		}else{
			return false;
		}
	}

	public void setWarningConnection(ConnectionKey key) throws Exception {
		byte[] result = client.getData().forPath(PathManager.getConnectionPath(key));
		ConnectionInfo info = CommonUtil.jsonToObject(encryption.decrypt(result), ConnectionInfo.class);
		info.setWarning(true);
		
		String data = CommonUtil.objectToJson(info);
		client.setData().forPath(PathManager.getConnectionPath(key), encryption.encrypt(data));
		connectionKeyMap.put(key, info);
		log.trace("connectionKeyMap put : " + key);
	}

	public void deleteConnection(ConnectionKey key) throws Exception{
		client.delete().guaranteed().forPath(PathManager.getConnectionPath(key));
		
		log.trace("connectionKeyMap before remove : " + connectionKeyMap);
		connectionKeyMap.remove(key);
		log.trace("connectionKeyMap after remove : " + connectionKeyMap);
		log.trace("connectionKeyMap remove : " + key);
		
		log.trace("SessionExpiredWatcherMap before remove : " + SessionExpiredWatcherMap);
		SessionExpiredWatcherMap.remove(key);
		log.trace("SessionExpiredWatcherMap after remove : "  + SessionExpiredWatcherMap);
		log.trace("SessionExpiredWatcherMap remove : " + key);

		log.info("deleteConnection : " + key);
	}
	
	private class DefaultListener implements CuratorListener{

		@Override
		public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
			log.debug("CuratorListener event.getType() : " + event.getType());
			log.debug("CuratorListener event.getPath() : " + event.getPath());
			log.debug("CuratorListener event.getName() : " + event.getName());
			log.debug("CuratorListener event.getResultCode() : " + event.getResultCode());
		}
		
	}
	
	private class ReConnectedListener implements ConnectionStateListener{

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			log.debug("ReConnectedListener newState : " + newState);
			switch(newState){
				
			case RECONNECTED:
				for(Entry<ConnectionKey, CuratorWatcher> keyValue : SessionExpiredWatcherMap.entrySet()){
					try {
						watchDataSource(keyValue.getKey(), keyValue.getValue());
						log.debug("re monitoring : " + keyValue.getKey());
					} catch (Exception e) {
						log.fatal("Exception by watchDataSource", e);
					}
				}
				
				for(Entry<ConnectionKey, ConnectionInfo> keyValue : connectionKeyMap.entrySet()){
					try {
						log.debug("re create connection node : " + keyValue.getKey());
						repairConnectionNode(keyValue.getKey(), keyValue.getValue());
					} catch (Exception e) {
						log.fatal("Exception by repairConnectionNode", e);
					}
				}
				
				break;
			default:
				break;
			}
			
		}
		
	}

	//	public JdbcRepositoryCurator(String connectString, RetryPolicy retryPolicy, ZookeeperTimeOutConf conf){
	//		this.client = CuratorFrameworkFactory.builder().
	//        connectString(connectString).
	//        sessionTimeoutMs(conf.getSessionTimeoutMs()).
	//        connectionTimeoutMs(conf.getConnectionTimeoutMs()).
	//        retryPolicy(retryPolicy).
	//        build();
	//		this.client.start();
	//		this.client.getCuratorListenable().addListener(new DefaultListener());
	//		this.client.getConnectionStateListenable().addListener(new ReConnectedListener());
	//	}
		
		public void close(){
			client.close();
		}
	
}
