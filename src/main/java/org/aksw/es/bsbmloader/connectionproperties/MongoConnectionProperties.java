package org.aksw.es.bsbmloader.connectionproperties;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.mongodb.MongoDbDataContext;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class MongoConnectionProperties extends ConnectionProperties {
	

	public void setConnectionProperties(String hostname, String port) throws Exception {
		setHostname(hostname);
		setPort(Integer.parseInt(port));
	}
	
	public UpdateableDataContext getDB(String name) throws Exception {
		MongoClient mongoClient = new MongoClient(getHostname(), getPort());
		DB databaseMongo = new DB(mongoClient, name);
		UpdateableDataContext dataContext = new MongoDbDataContext(databaseMongo);
		return dataContext;
	}
	
	public UpdateableDataContext getDBwriteConcern(String name) throws Exception {
		MongoClient mongoClient = new MongoClient(getHostname(), getPort());
		DB databaseMongo = new DB(mongoClient, name);
//		mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		UpdateableDataContext dataContext = new MongoDbDataContext(databaseMongo);
		return dataContext;
	}


}
