package org.aksw.es.bsbmloader.connectionproperties;

import org.aksw.es.bsbmloader.connectionbuilder.ConnectionDatabase;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.mongodb.MongoDbDataContext;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

public class MongoConnectionProperties extends ConnectionProperties implements ConnectionDatabase {
	private String hostname;
	private int port;
	private String name;

	public void setConnectionProperties(String hostname, String port) throws Exception {
		this.hostname = hostname;
		this.port = Integer.parseInt(port);
	}

	public UpdateableDataContext getDB() throws Exception {
		MongoClient mongoClient = new MongoClient(hostname, port);
		DB databaseMongo = new DB(mongoClient, name);
		mongoClient.setWriteConcern(WriteConcern.UNACKNOWLEDGED);
		UpdateableDataContext dataContext = new MongoDbDataContext(databaseMongo);
		return dataContext;
	}
	
	@Deprecated
	public UpdateableDataContext getDB(String name) throws Exception {
		MongoClient mongoClient = new MongoClient(hostname, port);
		DB databaseMongo = new DB(mongoClient, name);
		mongoClient.setWriteConcern(WriteConcern.UNACKNOWLEDGED);
		UpdateableDataContext dataContext = new MongoDbDataContext(databaseMongo);
		return dataContext;
	}

	@Deprecated
	public UpdateableDataContext getDBwriteConcern(String name) throws Exception {
		MongoClient mongoClient = new MongoClient(hostname, port);
		DB databaseMongo = new DB(mongoClient, name);
		// mongoClient.setWriteConcern(WriteConcern.ACKNOWLEDGED);
		UpdateableDataContext dataContext = new MongoDbDataContext(databaseMongo);
		return dataContext;
	}

	public void setConnectionProperties(String jdbcurl, String user, String password) throws Exception {
		throw new Exception("Unsupported");

	}

	public void setDatabaseName(String name) {
		this.name = name;

	}

}
