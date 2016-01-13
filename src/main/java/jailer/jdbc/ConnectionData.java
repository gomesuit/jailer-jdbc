package jailer.jdbc;

import jailer.core.model.ConnectionKey;
import jailer.core.model.PropertyContents;

import java.util.HashMap;
import java.util.Map;

public class ConnectionData extends ConnectionKey{
	private String jailerUrl;
	private String databaseUrl;
	private Map<String, PropertyContents> propertyList = new HashMap<>();
	private Map<String, String> optionalParam;
	
	public String getJailerUrl() {
		return jailerUrl;
	}
	public void setJailerUrl(String jailerUrl) {
		this.jailerUrl = jailerUrl;
	}
	public String getDatabaseUrl() {
		return databaseUrl;
	}
	public void setDatabaseUrl(String databaseUrl) {
		this.databaseUrl = databaseUrl;
	}
	public Map<String, PropertyContents> getPropertyList() {
		return propertyList;
	}
	public void setPropertyList(Map<String, PropertyContents> propertyList) {
		this.propertyList = propertyList;
	}
	public Map<String, String> getOptionalParam() {
		return optionalParam;
	}
	public void setOptionalParam(Map<String, String> optionalParam) {
		this.optionalParam = optionalParam;
	}
	
	@Override
	public String toString() {
		return "ConnectionData [jailerUrl=" + jailerUrl + ", databaseUrl="
				+ databaseUrl + ", propertyList=" + propertyList
				+ ", optionalParam=" + optionalParam + ", toString()="
				+ super.toString() + "]";
	}
}
