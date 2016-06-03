package org.aksw.es.bsbmloader.connectionproperties;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.couchdb.CouchDbDataContext;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

public class CouchConnectionProperties extends ConnectionProperties {

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

}
