package org.bsbmloader.metamodell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.summary.Product;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.data.RowBuilder;
import org.apache.metamodel.insert.InsertInto;
import org.apache.metamodel.insert.RowInsertable;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.mongodb.MongoDbDataContext;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.bsbmloader.helpClass.ProductHelper;

import com.mongodb.DB;
import com.mongodb.MongoClient;

/**
 * @author Tobias
 */
public class MongoDBLoader {
	private DB db;
	private String hostname;
	private int port;
	private String username;
	private String password;
	private MongoDbDataContext dc;
	MongoClient mongoClient;
	
	/**
	 * Throw error if collection person exist.
	 * All numbers are saved as String
	 * TODO Delete collection person before run function
	 * TODO Parse String to int
	 * TODO Refactor 
	 **/
	public void insertPersons(ArrayList<String[]> value){
		final Schema defaultSchema = setSchema();
		dc.executeUpdate(new UpdateScript() {
			private ArrayList<String[]> data;
			private Schema defaultSchema;
			
			public void run(UpdateCallback callback) {
				Table table = callback.createTable(defaultSchema,"person").withColumn("_id").withColumn("name").withColumn("mbox_sha1sum").withColumn("country")
						      .withColumn("publisher").withColumn("publishDate").execute();
				for(int index = 0; index < data.size();index++){
					String[] tmp = data.get(index);
					callback.insertInto(table).value("_id", tmp[0]).value("name", tmp[1]).value("mbox_sha1sum", tmp[2]).value("country", tmp[3])
					                          .value("publisher", tmp[4]).value("publishDate", tmp[5]).execute();
				}	
			}
			
			private UpdateScript init(ArrayList<String[]> value, Schema defaultSchema){
				this.data = value;
				this.defaultSchema = defaultSchema;
				return this;
			}
			
		}.init(value,defaultSchema));
	}
	
	
	/**
	 * Untest
	 * 
	 * @param data
	 */
	public void insertProduct(ArrayList<ProductHelper> data){
		final Schema defaultSchema = setSchema();
		dc.executeUpdate(new UpdateScript() {
			private ArrayList<ProductHelper> data; 
			private Schema defaultSchema;
			private RowInsertionBuilder rows ;
			
			public void run(UpdateCallback callback) {
				String[] tags = {"_id", "label","comment","producter","propertyNum1","propertyNum2","propertyNum3","propertyNum4","propertyNum5",
				        "propertyNum6","propertyTex1","propertyTex2","propertyTex3","propertyTex4","propertyTex5","propertyTex6",
				        "publisher","publishDate"};
				TableCreationBuilder tableCreation = callback.createTable(defaultSchema, "product");
				for (int i = 0; i < tags.length; i ++){
					tableCreation.withColumn(tags[i]);
				}
				Table table = tableCreation.execute();
				String[] producerTags = data.get(0).getProducerTags();
				for(int i = 0; i < data.size(); i++){
					Map<String, Object> nestedObj = new HashMap<String, Object>();
					for(int indexNestedObj = 0; indexNestedObj < data.get(i).getProducer().length; indexNestedObj++){
						nestedObj.put(producerTags[indexNestedObj], data.get(i).getProducer()[indexNestedObj]);
					};
					for(int indexInsert = 0 ; indexInsert < data.get(i).getValue().length;indexInsert++){
						if(indexInsert != 3){
							rows = callback.insertInto("table").value(tags[indexInsert],data.get(i).getValue()[indexInsert] );
						}else{
							rows = callback.insertInto("table").value(tags[3],nestedObj);
						}
					}
					
				  rows.execute();
				}
				
				
			}
			
			private UpdateScript init (ArrayList<ProductHelper> value, Schema defaultSchema){
				this.defaultSchema = defaultSchema;
				this.data = value;
				return this;
			}
		}.init(data, defaultSchema));
		
		
	}
	
	public void setConnectionProperties( String hostname, String port){
		this.hostname = hostname;
		this.port = Integer.parseInt(port);
	}
	
	private Schema setSchema(){
		mongoClient = new MongoClient(hostname,port);
		db = mongoClient.getDB("bsbm");
		dc = new MongoDbDataContext(db);
		return dc.getDefaultSchema();
	}

}
