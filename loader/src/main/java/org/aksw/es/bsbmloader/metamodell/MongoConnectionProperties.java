package org.aksw.es.bsbmloader.metamodell;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.mongodb.MongoDbDataContext;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class MongoConnectionProperties extends ConnectionProperties {
	private DB db;
	private UpdateableDataContext dc;

	public void setConnectionProperties(String hostname, String port) throws Exception {
		setHostname(hostname);
		setPort(Integer.parseInt(port));
	}
	
	public void deleteDatabase(){
		MongoClient mongoClient = new MongoClient(getHostname(), getPort());
		MongoDatabase mdb =  mongoClient.getDatabase("bsbm");	
		mdb.drop();
	}

	public UpdateableDataContext getDB() throws Exception {
		MongoClient mongoClient = new MongoClient(getHostname(), getPort());
		db = mongoClient.getDB("bsbm");
		dc = new MongoDbDataContext(db);
		return dc;
	}

}
