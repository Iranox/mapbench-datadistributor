package org.bsbmloader.metamodell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.mongodb.MongoDbDataContext;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.bsbmloader.helpClass.ProductHelper;
import org.bsbmloader.helpClass.ReviewHelper;

import com.mongodb.DB;
import com.mongodb.MongoClient;

/**
 * @author Tobias
 */
public class MongoDBLoader{
	private DB db;
	private String hostname;
	private int port;
	private String username;
	private String password;
	private MongoDbDataContext dc;
	MongoClient mongoClient;
	

	public void insertProductType(ArrayList<String[]> value){
		String[] tags = {"_id","label","comment","parent","publisher","publishDate"};
		runUpdate(value,tags,"producttype_details");
	}
	
	public void insertProductFeature(ArrayList<String[]> value){
		String[] tags = {"_id","label","comment","publisher","publishDate"};
		runUpdate(value,tags,"productfeature_details");
	}

	public void insertVendor(ArrayList<String[]> value){
		String[] tags = {"_id","label","comment","homepage","country","publisher","publishDate"};
		runUpdate(value,tags,"vendor");
	}
	
	public void insertPersons(ArrayList<String[]> value){
		String[] tags = {"_id","name","mbox_sha1sum","country","publisher","publishDate"};
		runUpdate(value,tags,"person");
	}
	
	public void insertReview(ReviewHelper data){
		final Schema defaultSchema = setSchema();
		String[] tmp1 = new String[data.getValue().get(0).length];
		ArrayList<String[]> value = new ArrayList<String[]>();
		for(int i = 0; i < data.getValue().size(); i++){
			for(int j = 0 ; j < data.getValue().size();j++){
				
				if(j == 2)
					tmp1[j] = data.getProductTitle(Integer.toString(j));
				else
					tmp1 [j]= data.getValue().get(i)[j];
				
				}
			value.add(tmp1);
		}
//		runUpdate(value,)
		
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
	
	private void runUpdate(ArrayList<String[]> data,String[] value, String tableName){
		final Schema defaultSchema = setSchema();
		dc.executeUpdate(new UpdateScript() {
			private String tableName;
			private String[] tags;
			private RowInsertionBuilder rows;
			private ArrayList<String[]> data;
			private Schema defaultSchema;

			public void run(UpdateCallback callback) {
			
				TableCreationBuilder  tableCreator = callback.createTable(defaultSchema, tableName);
				for(int i = 0; i < tags.length; i++){
					tableCreator.withColumn(tags[i]);
				}
				Table table = tableCreator.execute();
				rows = callback.insertInto(table);
				for(int i = 0; i < data.size();i++){
					for(int j = 0; j < tags.length; j++){
						rows.value(tags[j], data.get(i)[j]);
					}
					rows.execute();
				}	
			}
			
			private UpdateScript init(ArrayList<String[]> data,String[] value, String tableName, Schema defaultSchema){
				this.data = data;
				this.tags = value;
				this.tableName = tableName;
				this.defaultSchema = defaultSchema;
				return this;
			}
			
		}.init(data,value,tableName,defaultSchema));
	}
	
	
	private Schema setSchema(){
		mongoClient = new MongoClient(hostname,port);
		db = mongoClient.getDB("bsbm");
		dc = new MongoDbDataContext(db);
		return dc.getDefaultSchema();
	}

}
