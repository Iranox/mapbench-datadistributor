package org.bsbmloader.metamodell;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.DataContextFactory;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.builder.TableFromBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.bsbmloader.helper.Helper;
import org.bsbmloader.main.Main;

public class MySQL {
	private Connection connection ;
	private String jdbcUrl;
	private String username;
	private String password;	
	private  DataContext dc;
	private ArrayList<Helper> data = new ArrayList<Helper>();
	private static org.apache.log4j.Logger log = Logger.getLogger(MySQL.class);

//	TODO Relationship
	public  ArrayList<Helper> readDataBase(){
		buildConnection();		
		Schema schema = dc.getSchemaByName("benchmark");
		schema.getTableByName("benchmark");
	    Table[] tables = schema.getTables();	   	    
	    for(Table table: tables){   	
	    	 	Helper	help = new Helper();
		    	help.setTable(table.getName());
		    	help.setColumns(table.getColumns());
		    	help.setRows(getRows(table.getName()));	
		    	data.add(help);	   
	    }    
	    closeConnection();
	    return data;
		
	}
	
	public void test(){
//		SELECT * FROM ( product p inner join producer pp on p.producer = pp.nr) inner join person on p.publisher = person.nr
		
		buildConnection();		
		Schema schema = dc.getSchemaByName("benchmark");
		schema.getTableByName("benchmark");
	    Table table = schema.getTableByName("product");
	    
	    dc.query().from("product").innerJoin("producer").on("producer", "nr").selectAll();
	    
	  
	        
	    closeConnection();
		
	}
	
	public void getComplexData(String tableName, String id){
		buildConnection();
		if(!tableName.equals("productfeatureproduct") && !tableName.equals("producttypeproduct")){
			Schema schema = dc.getSchemaByName("benchmark");
			schema.getTableByName("benchmark");
			Table table = schema.getTableByName(tableName);
			for(Column relation: table.getForeignKeys()){
				log.info(relation.getName());
				if(!relation.getName().equals("publisher")){	
					DataSet ds = dc.query().from(relation.getName()).selectAll().where("nr").eq(id).execute();
					while(ds.next()){
						log.info(ds.getRow().toString());
					}
				}
				
					
				}
			
		}
	
		
		closeConnection();
	}
	
	
	public void setConnectionProperties(String jdbcUrl, String username, String password){
		this.jdbcUrl = jdbcUrl;
		this.username = username;
		this.password = password;
	}
	

	
	private void buildConnection(){
		try{
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(jdbcUrl,username,password);
			dc = DataContextFactory.createJdbcDataContext(connection);
		}catch (SQLException e) { 
			   e.printStackTrace(); 
        }catch(ClassNotFoundException e){
			 e.printStackTrace(); 
		}
	}
	
	private void closeConnection(){
		try{
			connection.close();
		}catch(SQLException e){
			e.printStackTrace();
		}
		
		
	}

	private ArrayList<Row> getRows(String tableName){
		ArrayList<Row> rows = new ArrayList<Row>();
		DataSet ds = dc.query().from("benchmark." + tableName).selectAll().execute();
		while(ds.next()){
			rows.add(ds.getRow());
		}
		ds.close();
		return rows;
	}
	


}
