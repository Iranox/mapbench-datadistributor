package org.aksw.es.bsbmloader.connectionproperties;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.jdbc.JdbcDataContext;
public class JdbcConnectionProperties extends ConnectionProperties {
	private String jdbcurl;
	
	public void setConnectionProperties(String jdbcurl, String user, String password) {
	    this.jdbcurl = jdbcurl;
		setUsername(user);
		setPassword(password);
	}

	
	public UpdateableDataContext getDB() throws Exception {
		Class.forName(getClassName(jdbcurl));
		Connection connection = DriverManager.getConnection(jdbcurl, getUsername(), getPassword());
		UpdateableDataContext dataContext = new JdbcDataContext(connection);
		return dataContext;

	}
	
	 private String getClassName(String jdbc){
	    	
	    	if(jdbc.contains("mysql")){
	    		return "com.mysql.jdbc.Driver";
	    	}
	    	
	    	if(jdbc.contains("postgresql")){
	    		return "org.postgresql.Driver";
	    	}
	    	
	    	
			return null;
		}
}
