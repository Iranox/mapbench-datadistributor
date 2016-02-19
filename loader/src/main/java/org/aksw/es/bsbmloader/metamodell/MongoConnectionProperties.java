package org.aksw.es.bsbmloader.metamodell;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.mongodb.MongoDbDataContext;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class MongoConnectionProperties extends ConnectionProperties {
	private UpdateableDataContext dc;

	public void setConnectionProperties(String hostname, String port) throws Exception {
		setHostname(hostname);
		setPort(Integer.parseInt(port));
	}
	

	public UpdateableDataContext getDB(String name) throws Exception {
		MongoClient mongoClient = new MongoClient(getHostname(), getPort());
		DB dba = new DB(mongoClient, name);
		dc = new MongoDbDataContext(dba);
		return dc;
	}

}
