package org.bsbmloader.metamodell;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.mongodb.MongoDbDataContext;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.bsbmloader.helpClass.OfferHelper;
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
	
	public void insertOffer(OfferHelper data){
		String[] tags = {"_id","product","producer","vendor","price","validFrom","validTo","deliveryDays","offerWebpage",
				  "publisher","publishDate"};
		String[] tmp1 = new String[data.getValue().get(0).length];
		ArrayList<String[]> value = new ArrayList<String[]>();
		for(int i = 0; i < data.getValue().size(); i++){
			tmp1= new String[14];
			for(int j = 0 ; j < 14;j++){
				if(j == 1){
					tmp1[j] = data.getProductTitle(data.getValue().get(i)[1]);
					continue;
				} 
				
				if(j == 2){
					tmp1[j] = data.getProducerTitle(data.getValue().get(i)[2]);
					continue;
				}
					
				
				if(j == 3){
					tmp1[j] = data.getVendorName(data.getValue().get(i)[3]);
					continue;
				}
					
				tmp1 [j]= data.getValue().get(i)[j];
				
				}
			value.add(tmp1);
		}
		runUpdate(value,tags,"offer");
		
	}
	
	
	public void insertReview(ReviewHelper data){
		String[] tags = {"_id","product","producer","person","reviewDate","title","text","language","rating1",
				"rating2","rating3","rating4","publisher","publishDate"};
		String[] tmp1 = new String[data.getValue().get(0).length];
		ArrayList<String[]> value = new ArrayList<String[]>();
		for(int i = 0; i < data.getValue().size(); i++){
			tmp1= new String[14];
			for(int j = 0 ; j < 14;j++){
				if(j == 1){
					tmp1[j] = data.getProductTitle(data.getValue().get(i)[1]);
					continue;
				} 
				
				if(j == 2){
					tmp1[j] = data.getProducerTitle(data.getValue().get(i)[2]);
					continue;
				}
					
				
				if(j == 3){
					tmp1[j] = data.getPersonName(data.getValue().get(i)[3]);
					continue;
				}
					
				tmp1 [j]= data.getValue().get(i)[j];
				
				}
			value.add(tmp1);
		}
		runUpdate(value,tags,"review");
		
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
				        "publisher","publishDate","productfeature","producttype"};
				TableCreationBuilder tableCreation = callback.createTable(defaultSchema, "product");
				for (int i = 0; i < tags.length; i ++){
					tableCreation.withColumn(tags[i]);
				}
				Table table = tableCreation.execute();
				String[] producerTags = data.get(0).getProducerTags();
				rows = callback.insertInto(table);
				for(int i = 0; i < data.size(); i++){
					Map<String, Object> nestedObj = new HashMap<String, Object>();
					for(int indexNestedObj = 0; indexNestedObj < data.get(i).getProducer().length; indexNestedObj++){
						nestedObj.put(producerTags[indexNestedObj], data.get(i).getProducer()[indexNestedObj]);
					};
					
					for(int indexInsert = 0 ; indexInsert < 20;indexInsert++){
						if(indexInsert == 3){
							rows.value(tags[3],nestedObj);
							continue;
							
						}
						
						if(indexInsert == 18){
							rows.value(tags[18], Arrays.asList(data.get(i).getProducefeatureArray()));
							continue;
						}
						
						if(indexInsert == 19){
							rows.value(tags[19], Arrays.asList(data.get(i).getProducetypeArray()));
							continue;
						}	
					
						rows.value(tags[indexInsert],data.get(i).getValue()[indexInsert] );
						
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
