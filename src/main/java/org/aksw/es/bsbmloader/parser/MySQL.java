  package org.aksw.es.bsbmloader.parser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.BlockingQueue;

import org.aksw.es.bsbmloader.posionrow.PosionRow;
import org.apache.log4j.Logger;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.DataContextFactory;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

@Deprecated
public class MySQL implements Runnable{
	private Connection connection;
	private String jdbcUrl;
	private String username;
	private String password;
	private String database;
	private DataContext dataContext;
	private BlockingQueue<Row> queue = null;
	private Table table; 
	private static org.apache.log4j.Logger log = Logger.getLogger(MySQL.class);
	

	public MySQL(BlockingQueue<Row> queue) {
		this.queue = queue;
	}

	public MySQL() {
	}
	
	public void setDatabase(String database){
		this.database = database;
	}
	


	public Table[] getTableMysql(String database) throws Exception{
		buildConnection();
		Schema schema = dataContext.getSchemaByName(database);
		Table[] tables = schema.getTables();
		closeConnection();
		return tables;
	}
	
	@Deprecated
	public Column[] getColumnMysql(String table, String database) throws Exception{
		buildConnection();
		Schema schema = dataContext.getSchemaByName(database);
		Table tableMySql = schema.getTableByName(table);
		Column[] columns = tableMySql.getColumns();
		closeConnection();
		
		return columns;
	}
	
	@Deprecated
	public void setConnectionProperties(String jdbcUrl, String username, String password) throws Exception {
		this.jdbcUrl = jdbcUrl;
		this.username = username;
		this.password = password;
	}

	public void run() {
		try{
			buildConnection();
			Schema schema = dataContext.getSchemaByName(database);
			Table tables = schema.getTableByName(table.getName());
			DataSet dataSet = dataContext.query().from(tables.getName()).selectAll().execute();	
			while(dataSet.next()){
				queue.put(dataSet.getRow());
				
			}
		  
			dataSet.close();
			queue.put(new PosionRow().getPosion());
			queue.put(new PosionRow().getPosion());
			queue.put(new PosionRow().getPosion());
			closeConnection();
		} catch(Exception e){
			log.info(e);
		}
		
		
	}


	public void setTable(Table table) {
		this.table = table;
	}
	
	@Deprecated
	private void closeConnection() throws Exception {
	
		connection.close();
	
	}

	@Deprecated
	private void buildConnection() throws Exception {
		Class.forName(getClassName(jdbcUrl));
		connection = DriverManager.getConnection(jdbcUrl, username, password);
		dataContext = DataContextFactory.createJdbcDataContext(connection);
	
	}
	
	@Deprecated
    private String getClassName(String jdbc){
    	
    	if(jdbc.contains("mysql")){
    		return "com.mysql.jdbc.Driver";
    	}
    	
    	if(jdbc.contains("postgresql")){
    		return "org.postgresql.Driver";
    	}
    	
    	
		return null;
	}
    


	

}
