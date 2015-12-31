package org.bsbmloader.loader;

import java.util.ArrayList;

import org.bsbmloader.helpClass.JSonObjectHelperClass;
import org.bsbmloader.helpClass.SimpleJSonHelpClass;
import org.bsbmloader.parser.JSonPaser;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;


public class Mongodb {
	private String hostname;
	private int port;
	private String username;
	private String password;
	
	
	public void insertDocument(String collection, ArrayList<SimpleJSonHelpClass> jsonString){
		MongoClient mongoClient = new MongoClient(hostname,port);
		DB db = mongoClient.getDB("bsbm");
		DBCollection coll = db.getCollection(collection);
		ArrayList<DBObject> docList = new ArrayList<DBObject>();
		for(int i = 0; i < jsonString.size();i++){
			 docList.add( (DBObject) JSON.parse(new JSonPaser().simpleJSON(jsonString.get(i))));
		}
		coll.insert(docList);
		
		
	}
	
	public void insertDocumentWithObject(String collection,ArrayList<JSonObjectHelperClass> jsonString){
		MongoClient mongoClient = new MongoClient(hostname,port);
		DB db = mongoClient.getDB("bsbm");
		DBCollection coll = db.getCollection(collection);
		ArrayList<DBObject> docList = new ArrayList<DBObject>();
		for(int i = 0; i < jsonString.size();i++){
			 docList.add( (DBObject) JSON.parse(new JSonPaser().JSONwithObject(jsonString.get(i))));
		}
		coll.insert(docList);
		
		
	}
	
	
	public void setConnection(String hostname, int port){
		this.hostname = hostname;
		this.port = port;
	}
	

}
