package org.aksw.es.bsbmloader.connectionbuilder;


import org.apache.metamodel.UpdateableDataContext;


public class ConnectionCreator {
	

		public UpdateableDataContext createConnection(String url,String user, String password, String type) throws Exception {
		ConnectionDatabase connection = null;
		if (url != null && url.contains("mysql")) {
			connection = new ConnectionBuilder().createConnectionProperties("mysql");
			connection.setConnectionProperties(url, user,
					password);
			return connection.getDB();
		}
		
		return null;
	}
	
	public UpdateableDataContext createConnection(String url,String user, String password,String database, String type) throws Exception {
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
