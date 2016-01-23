package jailer.jdbc.model;

import jailer.core.model.JailerDataSource;

import java.sql.Connection;

public class ConnectionCapsule {
	private Connection connection;
	private JailerDataSource jailerDataSource;
	
	public ConnectionCapsule(Connection connection,
			JailerDataSource jailerDataSource) {
		super();
		this.connection = connection;
		this.jailerDataSource = jailerDataSource;
	}

	public Connection getConnection() {
		return connection;
	}
	
	public JailerDataSource getJailerDataSource() {
		return jailerDataSource;
	}
}
