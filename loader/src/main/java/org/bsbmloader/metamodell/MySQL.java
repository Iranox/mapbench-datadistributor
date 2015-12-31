package org.bsbmloader.metamodell;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.DataContextFactory;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.bsbmloader.helpClass.JSonObjectHelperClass;
import org.bsbmloader.helpClass.SimpleJSonHelpClass;

public class MySQL {
	private Connection connection ;
	private String jdbcUrl;
	private String username;
	private String password;
	
	private  DataContext dc;
	private ArrayList<SimpleJSonHelpClass> data;
	private String[] tags;

	public ArrayList<SimpleJSonHelpClass> getAllPersons(){
		String[] tags = {"nr", "name","mbox_sha1sum","country","publisher","publishDate"};
		String[] value = new String[6];
		data = new ArrayList<SimpleJSonHelpClass>();
		buildConnection();
		try{	
			Query query = createSimpleQuery("person");
			DataSet ds = dc.executeQuery(query);
			while(ds.next()){
				value  = new String[tags.length];
				readDataFromRow(ds.getRow(), value);
				data.add(new SimpleJSonHelpClass(tags, value));
			}
			ds.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return data;	
	}
	
/**	public ArrayList<SimpleJSonHelpClass> getAllProducer(){
		String[] tags = {"nr", "label","comment","homepage","country","publisher","publishDate"};
		data = new ArrayList<SimpleJSonHelpClass>();
		buildConnection();
		try{	
			Query query = createSimpleQuery("producer");
			DataSet ds = dc.executeQuery(query);
			while(ds.next()){
				String[] value = new String[tags.length];
				readDataFromRow(ds.getRow(), value);
				data.add(new SimpleJSonHelpClass(tags, value));
			}
			ds.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return data;	
	}**/
	
	public ArrayList<JSonObjectHelperClass> getAllProduct(){
		String[] tags = {"nr", "label","comment","producter","propertyNum1","propertyNum2","propertyNum3","propertyNum4","propertyNum5",
				        "propertyNum6","propertyTex1","propertyTex2","propertyTex3","propertyTex4","propertyTex5","propertyTex6",
				        "publisher","publishDate"};
	    ArrayList<JSonObjectHelperClass>	data = new ArrayList<JSonObjectHelperClass>();
		buildConnection();
		try{	
			Query query = createSimpleQuery("product");
			DataSet ds = dc.executeQuery(query);
			while(ds.next()){
			    String[] value  = new String[tags.length];
				readDataFromRow(ds.getRow(), value);
				SimpleJSonHelpClass tmp = getProducer(ds.getRow().getValue(0).toString()).get(0);
				JSonObjectHelperClass jsObject = new JSonObjectHelperClass(tags, value, tmp);
				jsObject.setObjectTag("producer");
				data.add(jsObject);
			}
			ds.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return data;
	}
	
	public void setConnectionProperties(String jdbcUrl, String username, String password){
		this.jdbcUrl = jdbcUrl;
		this.username = username;
		this.password = password;
	}
	
	private ArrayList<SimpleJSonHelpClass> getProducer(String productNumber){
		String[] tags = {"nr", "label","comment","homepage","country","publisher","publishDate"};
		String[] value = new String[7];
		data = new ArrayList<SimpleJSonHelpClass>();
		buildConnection();
		try{	
			Query query = createSimpleQuery("producer",1);
			DataSet ds = dc.executeQuery(query);
			while(ds.next()){
				readDataFromRow(ds.getRow(), value);
				data.add(new SimpleJSonHelpClass(tags, value));
			}
			ds.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return data;	
	}

	private Query createSimpleQuery(String tableName, int id){
		    dc = DataContextFactory.createJdbcDataContext(connection);
			Query query = dc.query().from(tableName).selectAll().where("nr").eq(id).toQuery();
			return query;
	    }

    private Query createSimpleQuery(String tableName){
    	dc = DataContextFactory.createJdbcDataContext(connection);
		Query query = dc.query().from(tableName).selectAll().toQuery();
		return query;
    }
	
	private void buildConnection(){
		try{
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(jdbcUrl,username,password);
		}catch (SQLException e) { 
			   e.printStackTrace(); 
        }catch(ClassNotFoundException e){
			 e.printStackTrace(); 
		}
	}
	
	private String[] readDataFromRow(Row row, String[] value){
		for(int index = 0; index < row.size(); index++){
			if(row.getValue(index)!= null)
			   value[index] = row.getValue(index).toString();
			else
				value[index] = "null";
		}
		return value;
	}

}
