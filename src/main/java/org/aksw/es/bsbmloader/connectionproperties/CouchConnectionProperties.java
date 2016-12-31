package org.aksw.es.bsbmloader.connectionproperties;

import org.aksw.es.bsbmloader.connectionbuilder.ConnectionDatabase;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.couchdb.CouchDbDataContext;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

public class CouchConnectionProperties extends ConnectionProperties implements ConnectionDatabase {

	public void setConnectionProperties(String hostname, String port) throws Exception {
		setHostname(hostname);
		setPort(Integer.parseInt(port));
	}
	

	public UpdateableDataContext getDB(String user, String password) throws Exception {
		HttpClient httpClient = new StdHttpClient.Builder().host(getHostname()).password(password).username(user).maxConnections(4).build();
		StdCouchDbInstance couchDbInstance = new StdCouchDbInstance(httpClient);
		
		UpdateableDataContext dataContext = new CouchDbDataContext(couchDbInstance);
		return dataContext;
	}


	public void setDatabaseName(String name) throws Exception {
		// TODO Auto-generated method stub
		
	}


	public UpdateableDataContext getDB() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}


	public void setConnectionProperties(String jdbcurl, String user, String password) throws Exception {
		// TODO Auto-generated method stub
		
	}

}
