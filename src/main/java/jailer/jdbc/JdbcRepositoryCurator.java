package jailer.jdbc;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import jailer.core.CommonUtil;
import jailer.core.JailerEncryption;
import jailer.core.JailerEncryptionImpl;
import jailer.core.PathManager;
import jailer.core.ZookeeperTimeOutConf;
import jailer.core.model.ConnectionInfo;
import jailer.core.model.ConnectionKey;
import jailer.core.model.DataSourceKey;
import jailer.core.model.JailerDataSource;

public class JdbcRepositoryCurator {
	private final CuratorFramework client;
	private final JailerEncryption encryption = new JailerEncryptionImpl();

	// Timeout
	private static final int default_sessionTimeoutMs = 6 * 1000;
	private static final int default_connectionTimeoutMs = 5 * 1000;
	
	// ExponentialBackoffRetry
	private static final int default_baseSleepTimeMs = 1000;
	private static final int default_maxRetries = 3;
	
	public JdbcRepositoryCurator(String connectString){
		this(connectString, getDefaultRetryPolicy(), getDefaultZookeeperTimeOutConf());
	}
	
	private static ZookeeperTimeOutConf getDefaultZookeeperTimeOutConf() {
		return new ZookeeperTimeOutConf(default_sessionTimeoutMs, default_connectionTimeoutMs);
	}

	private static RetryPolicy getDefaultRetryPolicy() {
		return new ExponentialBackoffRetry(default_baseSleepTimeMs, default_maxRetries);
	}

	public JdbcRepositoryCurator(String connectString, RetryPolicy retryPolicy, ZookeeperTimeOutConf conf){
		this.client = CuratorFrameworkFactory.builder().
        connectString(connectString).
        sessionTimeoutMs(conf.getSessionTimeoutMs()).
        connectionTimeoutMs(conf.getConnectionTimeoutMs()).
        retryPolicy(retryPolicy).
        build();
		this.client.getCuratorListenable().addListener(new DefaultListener());
		this.client.getConnectionStateListenable().addListener(new ReConnectedListener());
		this.client.start();
	}
	
	public void close(){
		client.close();
	}
	
	public JailerDataSource getJailerDataSource(DataSourceKey key) throws Exception{
		byte[] result = client.getData().forPath(PathManager.getDataSourcePath(key));
		return CommonUtil.jsonToObject(encryption.decoded(result), JailerDataSource.class);
	}
	
	public boolean isExistsConnectionNode(ConnectionKey key) throws Exception{
		Stat stat = client.checkExists().forPath(PathManager.getConnectionPath(key));
		
		if(stat != null){
			return true;
		}else{
			return false;
		}
	}
	
	public ConnectionKey registConnection(DataSourceKey key, ConnectionInfo info) throws Exception{
		String data = CommonUtil.objectToJson(info);
		String connectionPath = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(PathManager.getDataSourcePath(key) + "/", encryption.encode(data));
		
		ConnectionKey connectionKey = new ConnectionKey();
		connectionKey.setServiceId(key.getServiceId());
		connectionKey.setGroupId(key.getGroupId());
		connectionKey.setDataSourceId(key.getDataSourceId());
		connectionKey.setConnectionId(connectionPath.substring(connectionPath.length() - 10, connectionPath.length()));
		
		connectionKeyMap.put(connectionKey, info);
		
		return connectionKey;
	}

	private Map<ConnectionKey, ConnectionInfo> connectionKeyMap = new ConcurrentHashMap<>();
	
	public void repairConnectionNode(ConnectionKey key, ConnectionInfo info) throws Exception{
		String data = CommonUtil.objectToJson(info);
		if(isExistsConnectionNode(key)){
			client.delete().forPath(PathManager.getConnectionPath(key));
		}
		client.create().withMode(CreateMode.EPHEMERAL).forPath(PathManager.getConnectionPath(key), encryption.encode(data));
	}
	
	public void deleteConnection(ConnectionKey key) throws Exception{
		client.delete().guaranteed().forPath(PathManager.getConnectionPath(key));
		connectionKeyMap.remove(key);
		System.out.println("remove前" + SessionExpiredWatcherMap);
		SessionExpiredWatcherMap.remove(key);
		System.out.println("remove後" + SessionExpiredWatcherMap);
	}
	
	private Map<ConnectionKey, CuratorWatcher> SessionExpiredWatcherMap = new ConcurrentHashMap<>();
	
	public void watchDataSource(ConnectionKey key, CuratorWatcher watcher) throws Exception{
		client.getData().inBackground(new MyBackgroundCallback(key, watcher)).forPath(PathManager.getDataSourcePath(key));
	}
	
	private class MyBackgroundCallback implements BackgroundCallback{
		private ConnectionKey key;
		private CuratorWatcher watcher;
		
		public MyBackgroundCallback(ConnectionKey key, CuratorWatcher watcher){
			this.key = key;
			this.watcher = watcher;
		}

		@Override
		public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
			client.checkExists().usingWatcher(watcher).forPath(PathManager.getDataSourcePath(key));
			SessionExpiredWatcherMap.put(key, watcher);
			System.out.println(SessionExpiredWatcherMap);
		}
		
	}
	
	public DataSourceKey getDataSourceKey(String uuid) throws Exception{
		byte[] result = client.getData().forPath(PathManager.getUuidPath(uuid));
		return CommonUtil.jsonToObject(encryption.decoded(result), DataSourceKey.class);
	}

	private class DefaultListener implements CuratorListener{

		@Override
		public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
			System.out.println("CuratorListener event.getType() : " + event.getType());
			System.out.println("CuratorListener event.getPath() : " + event.getPath());
			System.out.println("CuratorListener event.getName() : " + event.getName());
			System.out.println("CuratorListener event.getResultCode() : " + event.getResultCode());
			
		}
		
	}
	
	private class ReConnectedListener implements ConnectionStateListener{

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			// TODO Auto-generated method stub
			System.out.println("ConnectionStateListener newState : " + newState);
			switch(newState){
				
			case RECONNECTED:
				for(Entry<ConnectionKey, CuratorWatcher> keyValue : SessionExpiredWatcherMap.entrySet()){
					try {
						watchDataSource(keyValue.getKey(), keyValue.getValue());
						System.out.println("再ウォッチ");
					} catch (Exception e) {
						e.printStackTrace();
						//発生しないはず
					}
				}
				
				for(Entry<ConnectionKey, ConnectionInfo> keyValue : connectionKeyMap.entrySet()){
					try {
						System.out.println("コネクションノード再生成");
						repairConnectionNode(keyValue.getKey(), keyValue.getValue());
					} catch (Exception e) {
						e.printStackTrace();
						//発生しないはず
					}
				}
				
				break;
			default:
				break;
			}
			
		}
		
	}
	
}
