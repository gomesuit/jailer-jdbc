package jailer.jdbc;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Date;
import java.util.Enumeration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.KeeperException;

import jailer.core.model.ConnectionInfo;
import jailer.core.model.ConnectionKey;
import jailer.core.model.DataSourceKey;
import jailer.core.model.JailerDataSource;
import jailer.core.model.PropertyContents;

public class JailerDriver implements Driver{
	private org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(JailerDriver.class);

	private Driver lastUnderlyingDriverRequested;
	
	private JdbcRepositoryCurator repository = null;

    private static final String URL_PREFIX = "jdbc:jailer://";
	
	// 生成済みのstatement数
	private Map<String, JailerDataSource> JailerDataSourceCache = new ConcurrentHashMap<>();
	
	static{
		try {
			DriverManager.registerDriver(new JailerDriver());
		} catch (SQLException e) {
			throw (RuntimeException) new RuntimeException("could not register jailerjdbc driver!").initCause(e);
		}
	}
	
	public Connection reCreateConnection(ConnectionKeyData connectionData, CuratorWatcher watcher) throws Exception{
		JailerDataSource jailerDataSource = repository.getJailerDataSourceWithWatch(connectionData, watcher);
//		if(!isChange(connectionData, jailerDataSource)){
//			return null;
//		}
		
		Class.forName(jailerDataSource.getDriverName());
		
		Properties info = getProperties(jailerDataSource);
		
		String realUrl = jailerDataSource.getUrl();
		
		Driver d = getUnderlyingDriver(realUrl);
		
		Connection newConnection = d.connect(realUrl, info);
		
		lastUnderlyingDriverRequested = d;
		
		JailerDataSourceCache.put(jailerDataSource.getUuid(), jailerDataSource);
		
		return newConnection;
	}
	
//	public boolean isChange(ConnectionKeyData connectionData, JailerDataSource newJailerDataSource){
//		// Driverの変更チェック
//		if(!connectionData.getInfo().getDriverName().equals(newJailerDataSource.getDriverName())){
//			return true;
//		}
//		
//		// urlの変更チェック
//		if(!connectionData.getInfo().getConnectUrl().equals(newJailerDataSource.getUrl())){
//			return true;
//		}
//		
//		// プロパティの変更チェック
//		if(connectionData.getInfo().getPropertyList().size() != newJailerDataSource.getPropertyList().size()){
//			return true;
//		}
//		for(Entry<String, PropertyContents> keyValue : connectionData.getInfo().getPropertyList().entrySet()){
//			if(!keyValue.getValue().equals(newJailerDataSource.getPropertyList().get(keyValue.getKey()))){
//				return true;
//			}
//		}
//		
//		return false;
//	}

	public ConnectionKeyData createConnection(DataSourceKey key, Map<String, String> optionalParam) throws Exception{
		JailerDataSource jailerDataSource = repository.getJailerDataSource(key);
		ConnectionInfo connectionInfo = createConnectionInfo(jailerDataSource, optionalParam);
		
		ConnectionKeyData connectionData = repository.registConnection(key, connectionInfo);
		log.info("createConnection : " + connectionData);
		return connectionData;
	}
	
	private ConnectionInfo createConnectionInfo(JailerDataSource jailerDataSource, Map<String, String> optionalParam){
		ConnectionInfo connectionInfo = new ConnectionInfo();
		connectionInfo.setSinceConnectTime(new Date());
		connectionInfo.setDriverName(jailerDataSource.getDriverName());
		connectionInfo.setConnectUrl(jailerDataSource.getUrl());
		connectionInfo.setHide(jailerDataSource.isHide());
		connectionInfo.setPropertyList(jailerDataSource.getPropertyList());
		connectionInfo.setOptionalParam(optionalParam);
		
		try {
			InetAddress inetAddress = InetAddress.getLocalHost();
			connectionInfo.setHost(inetAddress.getHostName());
			connectionInfo.setIpAddress(inetAddress.getHostAddress());
		} catch (UnknownHostException e) {
			log.error("UnknownHostException", e);
			connectionInfo.setHost("UnknownHost");
			connectionInfo.setIpAddress("UnknownHostAddress");
		}
		
		return connectionInfo;
	}
	
	public void deleteConnection(ConnectionKey key) throws Exception{
		repository.deleteConnection(key);
		log.info("deleteConnection : " + key);
	}
	
	public void dataSourceWatcher(ConnectionKey key, CuratorWatcher watcher) throws Exception{
		repository.watchDataSource(key, watcher);
	}
	
	public void setWarningConnection(ConnectionKey key) throws Exception{
		repository.setWarningConnection(key);
	}
	
	private JailerDataSource getJailerDataSource(URI uri) throws Exception{
		String connectString = JailerJdbcURIManager.getConnectString(uri);
		
		// synchronized
		this.repository = getRepository(connectString);
		String uuid = JailerJdbcURIManager.getUUID(uri);
		try{
			DataSourceKey key = repository.getDataSourceKey(uuid);
			JailerDataSource jailerDataSource = repository.getJailerDataSource(key);
			JailerDataSourceCache.put(uuid, jailerDataSource);
			return jailerDataSource;
		}catch(KeeperException e){
			return JailerDataSourceCache.get(uuid);
		}
	}
	
	synchronized private JdbcRepositoryCurator getRepository(String connectString){
		if(this.repository == null){
			return new JdbcRepositoryCurator(connectString);
		}else{
			return this.repository;
		}
	}
	
	private Properties getProperties(JailerDataSource jailerDataSource){
		Properties info = new Properties();
		
		for(Entry<String, PropertyContents> keyValue : jailerDataSource.getPropertyList().entrySet()){
			info.put(keyValue.getKey(), keyValue.getValue().getValue());
		}
		
		return info;
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		URI jailerJdbcURI = getjailerJdbcURIByOverrideMethod(url);
		JailerDataSource jailerDataSource = getJailerDataSourceByOverrideMethod(jailerJdbcURI);
		loadDriverClassByOverrideMethod(jailerDataSource.getDriverName());
		
		String realUrl = jailerDataSource.getUrl();
		Driver d = DriverManager.getDriver(realUrl);
		if (d != null) {
			lastUnderlyingDriverRequested = d;
		}
		
		info = getProperties(jailerDataSource);
		
		try {
			DataSourceKey key = repository.getDataSourceKey(JailerJdbcURIManager.getUUID(jailerJdbcURI));
			ConnectionKeyData connectionData = createConnection(key, JailerJdbcURIManager.getParameterMap(jailerJdbcURI));
			return new JailerConnection(d.connect(realUrl, info), this, connectionData);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
	
	@Override
	public boolean acceptsURL(String url) throws SQLException {
		if(!url.startsWith(URL_PREFIX)){
			return false;
		}
		
		URI jailerJdbcURI = getjailerJdbcURIByOverrideMethod(url);
		JailerDataSource jailerDataSource = getJailerDataSourceByOverrideMethod(jailerJdbcURI);
		loadDriverClassByOverrideMethod(jailerDataSource.getDriverName());
		
		String realUrl = jailerDataSource.getUrl();
		Driver d = getUnderlyingDriver(realUrl);
		if (d != null) {
			lastUnderlyingDriverRequested = d;
			return true;
		}
		
		return false;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		URI jailerJdbcURI = getjailerJdbcURIByOverrideMethod(url);
		JailerDataSource jailerDataSource = getJailerDataSourceByOverrideMethod(jailerJdbcURI);
		loadDriverClassByOverrideMethod(jailerDataSource.getDriverName());
		
		String realUrl = jailerDataSource.getUrl();
		Driver d = getUnderlyingDriver(realUrl);
		if (d != null) {
			lastUnderlyingDriverRequested = d;
		}

		info = getProperties(jailerDataSource);
		
		return lastUnderlyingDriverRequested.getPropertyInfo(realUrl, info);
	}

	@Override
	public int getMajorVersion() {
		if (lastUnderlyingDriverRequested == null) {
			return 1;
		} 
		return lastUnderlyingDriverRequested.getMajorVersion();
	}

	@Override
	public int getMinorVersion() {
		if (lastUnderlyingDriverRequested == null) {
			return 0;
		}
		return lastUnderlyingDriverRequested.getMinorVersion();
	}

	@Override
	public boolean jdbcCompliant() {
		return lastUnderlyingDriverRequested != null &&
				   lastUnderlyingDriverRequested.jdbcCompliant();
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return lastUnderlyingDriverRequested.getParentLogger();
	}
	
	private Driver getUnderlyingDriver(String url) throws SQLException{
		log.trace("getUnderlyingDriver(String url) url = " + url);
		Enumeration<Driver> e = DriverManager.getDrivers();

		Driver d;
		while (e.hasMoreElements()) {
			d = e.nextElement();
			log.trace("check driver class of getUnderlyingDriver() = " + d.getClass().getName());
			if(d.getClass().getName().equals(this.getClass().getName())){
				log.trace("check driver class is mine. continue. ClassName = " + d.getClass().getName());
				continue;
			}

			if (d.acceptsURL(url)) {
				return d;
			}
		}
		
		return null;
	}

	private URI getjailerJdbcURIByOverrideMethod(String url) throws SQLException {
		try {
			return JailerJdbcURIManager.getUri(url);
		} catch (URISyntaxException e) {
			throw new SQLException(e);
		}
	}

	private JailerDataSource getJailerDataSourceByOverrideMethod(URI jailerJdbcURI) throws SQLException {
		try {
			return getJailerDataSource(jailerJdbcURI);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	private void loadDriverClassByOverrideMethod(String className) throws SQLException{
		try {
			Class.forName(className);
		} catch (ClassNotFoundException e) {
			log.error("LoadDriverClass ClassNotFoundException : " + className, e);
			throw new SQLException(e);
		}
	}

}
