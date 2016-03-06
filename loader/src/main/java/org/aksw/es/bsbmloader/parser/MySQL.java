package org.aksw.es.bsbmloader.parser;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.DataContextFactory;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class MySQL implements Runnable{
	private Connection connection;
	private String jdbcUrl;
	private String username;
	private String password;
	private String database;
	private DataContext dataContext;
	protected BlockingQueue<Row> queue = null;
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
	
	public Column[] getColumnMysql(String table, String database) throws Exception{
		buildConnection();
		Schema schema = dataContext.getSchemaByName(database);
		Table tableMySql = schema.getTableByName(table);
		Column[] columns = tableMySql.getColumns();
		closeConnection();
		
		return columns;
	}
	
	public ArrayList<String> getFkTable(String table, String database) throws Exception{
		buildConnection();
		 ArrayList<String> forgeinTable = new ArrayList<String>();
		Schema schema = dataContext.getSchemaByName(database);	
		Table tableMySql = schema.getTableByName(table);
		for(Relationship relation : tableMySql.getPrimaryKeyRelationships()){
			forgeinTable.add(relation.getForeignTable().getName());
		}
		closeConnection();
		return forgeinTable;
	}
	


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
			closeConnection();
		} catch(Exception e){
			log.info(e);
		}
		
		
	}


	public void setTable(Table table) {
		this.table = table;
	}

	private void closeConnection() throws Exception {
	
		connection.close();
	
	}

	private void buildConnection() throws Exception {
		getFile(jdbcUrl);
		URL u = new URL("file:"+  getFile(jdbcUrl));
		String classname = getClassName(jdbcUrl);
		URLClassLoader ucl = new URLClassLoader(new URL[] { u });
		Driver d = (Driver)Class.forName(classname, true, ucl).newInstance();
		DriverManager.registerDriver(new DriverLoader(d));
		connection = DriverManager.getConnection(jdbcUrl, username, password);
		dataContext = DataContextFactory.createJdbcDataContext(connection);
	
	}
	
    private String getClassName(String jdbc){
    	
    	if(jdbc.contains("mysql")){
    		return "com.mysql.jdbc.Driver";
    	}
    	
    	if(jdbc.contains("postgresql")){
    		return "org.postgresql.Driver";
    	}
    	
    	
		return null;
	}
    
    private String getFile(String jdbc){
    	String[] db = jdbc.split(":");
    	File[] files = new File("src/lib").listFiles();
    	for (File file : files) {
    	    if (file.isFile() && file.getName().contains(db[1])) {
//    	    	System.out.println(file.getAbsolutePath());
    	        return file.getAbsolutePath();
    	    }
    	}
    	return null;
    }

	

}
