package jailer.jdbc;

import java.util.HashMap;
import java.util.Map;

import jailer.core.model.ConnectionKey;
import jailer.core.model.PropertyContents;

public class ConnectionData extends ConnectionKey{
	private String url;
	private Map<String, PropertyContents> propertyList = new HashMap<>();
	private Map<String, String> optionalParam;
	
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
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
		return "ConnectionData [url=" + url + ", toString()="
				+ super.toString() + "]";
	}
}
