package jailer.jdbc;

import jailer.core.model.ConnectionInfo;
import jailer.core.model.ConnectionKey;
import jailer.core.model.DataSourceKey;
import jailer.core.model.JailerDataSource;
import jailer.jdbc.model.ConnectionCapsule;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

public class JailerConnection implements Connection{
	private Logger log = Logger.getLogger(JailerConnection.class);

	private final JailerDriver driver;
	private final JdbcRepositoryCurator repository;
	private final URI jailerJdbcURI;
	private final DataSourceKey key;
	
	private ConnectionCapsule realConnectionCapsule;
	
	private String connectionId;
	
	// 生成済みのstatement数
	private Map<Statement, Statement> statementMap = new ConcurrentHashMap<>();
	
	private static final int RELEASE_STATEMENT_TiMEOUT_SEC = 5;
	
	public JailerConnection(ConnectionCapsule realConnectionCapsule, JailerDriver driver, JdbcRepositoryCurator repository, DataSourceKey key, URI jailerJdbcURI) throws Exception{
		this.realConnectionCapsule = realConnectionCapsule;
		this.driver = driver;
		this.repository = repository;
		this.jailerJdbcURI = jailerJdbcURI;
		this.key = key;
		this.connectionId = createConnection(this.realConnectionCapsule.getJailerDataSource());
		this.repository.watchDataSource(new ConnectionKey(this.key, this.connectionId), new DataSourceWatcher());
	}
	
	public void reduceStatementNumber(Statement statement){
		statementMap.remove(statement);
	}
	
	public void addStatementNumber(Statement statement){
		statementMap.put(statement, statement);
	}

	private String createConnection(JailerDataSource jailerDataSource) throws Exception{
		ConnectionInfo info = createConnectionInfo(jailerDataSource);
		return repository.registConnection(this.key, info);
	}

	private ConnectionInfo createConnectionInfo(JailerDataSource jailerDataSource){
		ConnectionInfo connectionInfo = new ConnectionInfo();
		connectionInfo.setSinceConnectTime(new Date());
		connectionInfo.setDriverName(jailerDataSource.getDriverName());
		connectionInfo.setConnectUrl(jailerDataSource.getUrl());
		connectionInfo.setHide(jailerDataSource.isHide());
		connectionInfo.setPropertyList(jailerDataSource.getPropertyList());
		connectionInfo.setOptionalParam(JailerJdbcURIManager.getParameterMap(jailerJdbcURI));
		
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
	
	private class DataSourceWatcher implements CuratorWatcher{

		@Override
		public void process(WatchedEvent event) throws Exception {
			if(realConnectionCapsule.getConnection().isClosed()) return;
			
			log.trace("Path : " + event.getPath());
			log.trace("key : " + connectionId);
			log.trace("EventType : " + event.getType());
			log.trace("KeeperState : " + event.getState());
			
			if(event.getType() == EventType.NodeDataChanged){
				
				// 新しいコネクションを生成
				ConnectionCapsule newConnectionCapsule = null;
				try{
					newConnectionCapsule = driver.reCreateConnection(key);
				}catch(Exception e){
					// コネクション生成失敗時の処理
					log.error("Error occurred by reCreateConnection !!", e);
					repository.setWarningConnection(new ConnectionKey(key, connectionId));
					return;
				}finally{
					repository.watchDataSource(new ConnectionKey(key, connectionId), new DataSourceWatcher());
				}
				
				if(realConnectionCapsule.getConnection().getAutoCommit()){

					// 旧コネクション退避
					Connection oldConnection = realConnectionCapsule.getConnection();
					
					// コネクション貼り替え
					realConnectionCapsule = newConnectionCapsule;
					
					// 新しいコネクションノードを生成
					String newConnectionId = createConnection(realConnectionCapsule.getJailerDataSource());
					
					// 生成済みのstatement数が0になるまで待機
					long start = System.currentTimeMillis();
					long end;
					long time;
					log.trace("Wait until the previously generated statement number becomes 0. : " + connectionId);
					while(statementMap.size() != 0){
						Thread.sleep(10);
						end = System.currentTimeMillis();
						time = (end - start) / 1000;
						if(time >= RELEASE_STATEMENT_TiMEOUT_SEC){
							for(Statement statement : statementMap.keySet()){
								log.error("releasing statement is timeout. close statement!! : " + connectionId);
								statement.close();
							}
						}
					}
					log.trace("The previously generated statement number becomes 0. : " + connectionId);
					
					// 旧コネクションクローズ
					oldConnection.close();
					
					// 旧コネクション接続情報退避
					String oldConnectionId = connectionId;
					
					// コネクション接続情報更新
					connectionId = newConnectionId;
					
					// 旧コネクション接続情報削除
					repository.deleteConnection(new ConnectionKey(key, oldConnectionId));
					
				}else{
					newConnectionCapsule.getConnection().close();
					realConnectionCapsule.getConnection().rollback();
					close();
				}
			}
		}
	}

	public Connection getRealConnection() {
		return realConnectionCapsule.getConnection();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return getRealConnection().unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return getRealConnection().isWrapperFor(iface);
	}

	@Override
	public Statement createStatement() throws SQLException {
		return new JailerStatement(getRealConnection().createStatement(), this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return new JailerPreparedStatement(getRealConnection().prepareStatement(sql), this);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		return new JailerCallableStatement(getRealConnection().prepareCall(sql), this);
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return getRealConnection().nativeSQL(sql);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		getRealConnection().setAutoCommit(autoCommit);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return getRealConnection().getAutoCommit();
	}

	@Override
	public void commit() throws SQLException {
		getRealConnection().commit();
	}

	@Override
	public void rollback() throws SQLException {
		getRealConnection().rollback();
	}

	@Override
	public void close() throws SQLException {
		getRealConnection().close();
		
		if(connectionId != null){
			try {
				repository.deleteConnection(new ConnectionKey(key, connectionId));
			} catch (Exception e) {
				log.error(e);
			}
		}
		
		connectionId = null;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return getRealConnection().isClosed();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
        return getRealConnection().getMetaData();
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		getRealConnection().setReadOnly(readOnly);
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return getRealConnection().isReadOnly();
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		getRealConnection().setCatalog(catalog);
	}

	@Override
	public String getCatalog() throws SQLException {
		return getRealConnection().getCatalog();
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		getRealConnection().setTransactionIsolation(level);
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return getRealConnection().getTransactionIsolation();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return getRealConnection().getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		getRealConnection().clearWarnings();
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return new JailerStatement(getRealConnection().createStatement(resultSetType, resultSetConcurrency), this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		return new JailerPreparedStatement(getRealConnection().prepareStatement(sql, resultSetType, resultSetConcurrency), this);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		return new JailerCallableStatement(getRealConnection().prepareCall(sql, resultSetType, resultSetConcurrency), this);
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return getRealConnection().getTypeMap();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		getRealConnection().setTypeMap(map);
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		getRealConnection().setHoldability(holdability);
	}

	@Override
	public int getHoldability() throws SQLException {
		return getRealConnection().getHoldability();
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		return getRealConnection().setSavepoint();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		return getRealConnection().setSavepoint(name);
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		getRealConnection().rollback(savepoint);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		getRealConnection().releaseSavepoint(savepoint);
	}

	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return new JailerStatement(getRealConnection().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability), this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return new JailerPreparedStatement(getRealConnection().prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability), this);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return new JailerCallableStatement(getRealConnection().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability), this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		return new JailerPreparedStatement(getRealConnection().prepareStatement(sql, autoGeneratedKeys), this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		return new JailerPreparedStatement(getRealConnection().prepareStatement(sql, columnIndexes), this);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		return new JailerPreparedStatement(getRealConnection().prepareStatement(sql, columnNames), this);
	}

	@Override
	public Clob createClob() throws SQLException {
		return getRealConnection().createClob();
	}

	@Override
	public Blob createBlob() throws SQLException {
		return getRealConnection().createBlob();
	}

	@Override
	public NClob createNClob() throws SQLException {
		return getRealConnection().createNClob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		return getRealConnection().createSQLXML();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return getRealConnection().isValid(timeout);
	}

	@Override
	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		getRealConnection().setClientInfo(name, value);
	}

	@Override
	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		getRealConnection().setClientInfo(properties);
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return getRealConnection().getClientInfo(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return getRealConnection().getClientInfo();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		return getRealConnection().createArrayOf(typeName, elements);
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		return getRealConnection().createStruct(typeName, attributes);
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		getRealConnection().setSchema(schema);
	}

	@Override
	public String getSchema() throws SQLException {
		return getRealConnection().getSchema();
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		getRealConnection().abort(executor);
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		getRealConnection().setNetworkTimeout(executor, milliseconds);
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		return getRealConnection().getNetworkTimeout();
	}

}
