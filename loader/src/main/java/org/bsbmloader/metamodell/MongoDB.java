package org.bsbmloader.metamodell;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.mongodb.MongoDbDataContext;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class MongoDB extends Loader{
	private DB db;
	private UpdateableDataContext dc;
	
	public void setConnectionProperties( String hostname, String port){
		setHostname(hostname);
		setPort(Integer.parseInt(port));
	}
	
	public UpdateableDataContext getDB(){
	MongoClient mongoClient = new MongoClient(getHostname(),getPort());
	db = mongoClient.getDB("bsbm");
	dc = new MongoDbDataContext(db);
	return dc;
}

}
