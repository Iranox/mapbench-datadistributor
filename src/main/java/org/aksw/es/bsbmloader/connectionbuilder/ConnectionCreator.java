package org.aksw.es.bsbmloader.connectionbuilder;


import org.apache.metamodel.UpdateableDataContext;


public class ConnectionCreator {
	

		public UpdateableDataContext createJDBCConnection(String url,String user, String password) throws Exception {
		ConnectionDatabase connection = null;
		if (url != null) {
			if( url.contains("mysql")){
				connection = new ConnectionBuilder().createConnectionProperties("mysql");
			}
			
			if( url.contains("h2")){
				connection = new ConnectionBuilder().createConnectionProperties("h2");
			}
			
			connection.setConnectionProperties(url, user,
					password);
			return connection.getDB();
		}
		
		return null;
	}
	
	public UpdateableDataContext createNoSQLConnection(String url,String user, String password,String database, String type) throws Exception {
		ConnectionDatabase connection = null;
		
		String urlAttribute[] = url.split(":");
		String port = urlAttribute[1];
		String host = urlAttribute[0];
		
	
		if (type.equals("mongodb")) {
			connection = new ConnectionBuilder().createConnectionProperties("mongodb");
			connection.setConnectionProperties(host,port);
			connection.setDatabaseName(database);
				
			return connection.getDB();
		}

		return null;

	}
	 
}
