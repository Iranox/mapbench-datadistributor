package org.aksw.es.bsbmloader.connectionproperties;

import java.sql.Connection;
import java.sql.DriverManager;

import org.aksw.es.bsbmloader.connectionbuilder.ConnectionDatabase;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.jdbc.JdbcDataContext;

public class JdbcConnectionProperties implements ConnectionDatabase {
	private String jdbcurl;
	private String user;
	private String password;

	public void setConnectionProperties(String jdbcurl, String user, String password) throws Exception {
		this.jdbcurl = jdbcurl;
		this.user = user;
		this.password = password;
	}

	public UpdateableDataContext getDB() throws Exception {
		Class.forName(getClassName(jdbcurl));
		Connection connection = DriverManager.getConnection(jdbcurl, user, password);
		connection.setAutoCommit(false);
		UpdateableDataContext dataContext = new JdbcDataContext(connection);
		return dataContext;

	}

	private String getClassName(String jdbc) {

		if (jdbc.contains("mysql")) {
			return "com.mysql.jdbc.Driver";
		}
		
		if (jdbc.contains("h2")) {
			return "org.h2.Driver";
		}


		if (jdbc.contains("postgresql")) {
			return "org.postgresql.Driver";
		}

		return null;
	}

	public void setConnectionProperties(String hostname, String port) throws Exception {
		throw new Exception("Unsupported");

	}

	public void setDatabaseName(String name) throws Exception {
		throw new Exception("Unsupported");
	}
}
