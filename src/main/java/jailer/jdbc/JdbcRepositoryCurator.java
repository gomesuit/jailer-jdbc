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
import org.apache.curator.retry.RetryNTimes;
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

public class JdbcRepositoryCurator {
	private Logger log = Logger.getLogger(JdbcRepositoryCurator.class);
	
	private final CuratorFramework client;
	private final JailerEncryption encryption = new JailerAESEncryption();

	// zookeeper timeout
	private static final int default_sessionTimeoutMs = 3 * 1000;
	private static final int default_connectionTimeoutMs = 2 * 1000;
	
	// RetryNTimes
	private static final int default_retry_times = 2;
	private static final int default_sleepMsBetweenRetries = 1000;
	
	private ConnectionState connectionState = ConnectionState.CONNECTED;
	
	// 再Watch用Map
	private Map<ConnectionKey, CuratorWatcher> SessionExpiredWatcherMap = new ConcurrentHashMap<>();
	
	// connectionノード再生成用
	private Map<ConnectionKey, ConnectionInfo> connectionKeyMap = new ConcurrentHashMap<>();
	
	public JdbcRepositoryCurator(String connectString){
		this(connectString, getDefaultRetryPolicy(), getDefaultZookeeperTimeOutConf());
	}

	private static ZookeeperTimeOutConf getDefaultZookeeperTimeOutConf() {
		return new ZookeeperTimeOutConf(default_sessionTimeoutMs, default_connectionTimeoutMs);
	}

	private static RetryPolicy getDefaultRetryPolicy() {
		return new RetryNTimes(default_retry_times, default_sleepMsBetweenRetries);
	}
	
	public JdbcRepositoryCurator(CuratorFramework client){
		this.client = client;
		this.client.getCuratorListenable().addListener(new DefaultListener());
		this.client.getConnectionStateListenable().addListener(new ReConnectedListener());
	}

	public JdbcRepositoryCurator(String connectString, RetryPolicy retryPolicy, ZookeeperTimeOutConf conf){
		this.client = CuratorFrameworkFactory.builder().
        connectString(connectString).
        sessionTimeoutMs(conf.getSessionTimeoutMs()).
        connectionTimeoutMs(conf.getConnectionTimeoutMs()).
        retryPolicy(retryPolicy).
        build();
		this.client.start();
		this.client.getCuratorListenable().addListener(new DefaultListener());
		this.client.getConnectionStateListenable().addListener(new ReConnectedListener());
	}
	
	private Map<String, DataSourceKey> dataSourceKeyCache = new ConcurrentHashMap<>();
	
	public DataSourceKey getDataSourceKey(String uuid) throws Exception{
		if(!isConnected()){
			return dataSourceKeyCache.get(uuid);
		}
		
		byte[] result = client.getData().forPath(PathManager.getUuidPath(uuid));
		log.trace("getDataSourceKey() path : " + PathManager.getUuidPath(uuid));
		DataSourceKey key = CommonUtil.jsonToObject(encryption.decrypt(result), DataSourceKey.class);
		dataSourceKeyCache.put(uuid, key);
		
		return key;
	}
	
	private Map<DataSourceKey, JailerDataSource> jailerDataSourceCache = new ConcurrentHashMap<>();
	
	public JailerDataSource getJailerDataSource(DataSourceKey key) throws Exception{
		if(!isConnected()){
			return jailerDataSourceCache.get(key);
		}
		
		byte[] result = client.getData().forPath(PathManager.getDataSourceCurrentPath(key));
		log.trace("getJailerDataSource() path : " + PathManager.getDataSourceCurrentPath(key));
		log.trace("getJailerDataSource() result : " + encryption.decrypt(result));
		JailerDataSource jailerDataSource = CommonUtil.jsonToObject(encryption.decrypt(result), JailerDataSource.class);
		jailerDataSourceCache.put(key, jailerDataSource);
		
		return jailerDataSource;
	}
	
	public void watchDataSource(ConnectionKey key, CuratorWatcher watcher) throws Exception{
		if(isConnected()){
			client.checkExists().usingWatcher(watcher).forPath(PathManager.getDataSourceCurrentPath(key));
		}
		
		SessionExpiredWatcherMap.put(key, watcher);
		log.trace("SessionExpiredWatcherMap put : " + key);
	}
	
	public String registConnection(DataSourceKey key, ConnectionInfo info) throws Exception{
		String data = CommonUtil.objectToJson(info);
		
		String connectionId = null;
		if(isConnected()){
			String connectionPath = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(PathManager.getDataSourceCurrentPath(key) + "/", encryption.encrypt(data));
			connectionId = (connectionPath.substring(connectionPath.length() - 10, connectionPath.length()));
		}else{
			connectionId = CommonUtil.getRandomUUID();
		}
		
		connectionKeyMap.put(new ConnectionKey(key, connectionId), info);
		log.trace("connectionKeyMap put : " + connectionId);
		
		return connectionId;
	}
	
	public void repairConnectionNode(ConnectionKey key, ConnectionInfo info) throws Exception{
		if(!isConnected()) return;
		
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
		ConnectionInfo info = null;
		if(isConnected()){
			byte[] result = client.getData().forPath(PathManager.getConnectionPath(key));
			info = CommonUtil.jsonToObject(encryption.decrypt(result), ConnectionInfo.class);
		}else{
			info = connectionKeyMap.get(key);
		}
		info.setWarning(true);
		
		String data = CommonUtil.objectToJson(info);
		if(isConnected()){
			client.setData().forPath(PathManager.getConnectionPath(key), encryption.encrypt(data));
		}
		connectionKeyMap.put(key, info);
		log.trace("connectionKeyMap put : " + key);
	}

	public void deleteConnection(ConnectionKey key) throws Exception{
		if(isConnected()){
			client.delete().guaranteed().forPath(PathManager.getConnectionPath(key));
		}
		
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
	
	private boolean isConnected(){
		if(connectionState == null) return false;
		if(connectionState == ConnectionState.LOST) return false;
		if(connectionState == ConnectionState.SUSPENDED) return false;
		return true;
	}
	
	private class ReConnectedListener implements ConnectionStateListener{

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			log.debug("ReConnectedListener newState : " + newState);
			connectionState = newState;
			
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
		
	public void close(){
		client.close();
	}
	
}
