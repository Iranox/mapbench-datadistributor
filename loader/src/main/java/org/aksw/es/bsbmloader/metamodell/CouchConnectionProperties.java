package org.aksw.es.bsbmloader.metamodell;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.couchdb.CouchDbDataContext;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

public class CouchConnectionProperties extends ConnectionProperties  {
	private UpdateableDataContext dc;
	private HttpClient httpClient; 
	private StdCouchDbInstance couchDbInstance;

	public void setConnectionProperties(String hostname, String port) throws Exception {
		setHostname(hostname);
		setPort(Integer.parseInt(port));
	}
	

	public UpdateableDataContext getDB(String user, String password) throws Exception {
		httpClient = new StdHttpClient.Builder().host(getHostname()).password(password).username(user).build();
		couchDbInstance = new StdCouchDbInstance(httpClient);
		dc = new CouchDbDataContext(couchDbInstance);
		return dc;
	}

}
