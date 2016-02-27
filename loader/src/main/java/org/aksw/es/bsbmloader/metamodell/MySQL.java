package org.aksw.es.bsbmloader.metamodell;

import java.sql.Connection;
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
	private DataContext dc;
	protected BlockingQueue<Row> queue = null;
	private Table table; 
	private String database;
	private static org.apache.log4j.Logger log = Logger.getLogger(MySQL.class);
	
	
	
	
	public MySQL(BlockingQueue<Row> queue) {
		this.queue = queue;
	}

	public MySQL() {
	
	}

	public Table[] getTableMysql(String database) throws Exception{
		buildConnection();
		Schema schema = dc.getSchemaByName(database);
		Table[] tables = schema.getTables();
		closeConnection();
		return tables;
	}
	
	public Column[] getColumnMysql(String table, String database) throws Exception{
		buildConnection();
		Schema schema = dc.getSchemaByName(database);
		Table tableMySql = schema.getTableByName(table);
		Column[] columns = tableMySql.getColumns();
		closeConnection();
		
		return columns;
	}
	
	public ArrayList<String> getFkTable(String table, String database) throws Exception{
		buildConnection();
		 ArrayList<String> forgeinTable = new ArrayList<String>();
		Schema schema = dc.getSchemaByName(database);	
		Table tableMySql = schema.getTableByName(table);
		for(Relationship relation : tableMySql.getPrimaryKeyRelationships()){
			forgeinTable.add(relation.getForeignTable().getName());
		}
		closeConnection();
		return forgeinTable;
	}
	
	public ArrayList<Row> getRowsMysql(Table table, String database) throws Exception{
		buildConnection();
		Schema schema = dc.getSchemaByName(database);
		Table tables = schema.getTableByName(table.getName());
		DataSet ds = dc.query().from(tables).selectAll().execute();
		ArrayList<Row> rows = new ArrayList<Row>();
		while(ds.next()){
			rows.add(ds.getRow());
			Thread.sleep(10);
		}
		ds.close();
		closeConnection();
		return rows;
		
	}

	public void setConnectionProperties(String jdbcUrl, String username, String password) throws Exception {
		this.jdbcUrl = jdbcUrl;
		this.username = username;
		this.password = password;
	}

	public void run() {
		try{
			buildConnection();
			Schema schema = dc.getSchemaByName("benchmark");
			Table tables = schema.getTableByName(table.getName());
			DataSet ds = dc.query().from(tables.getName()).selectAll().execute();	
			while(ds.next()){
				if(queue.size() == 900){
					Thread.sleep(100);
				}
				queue.add(ds.getRow());
			}
		  
			ds.close();
			queue.add(new PosionRow().getPosion());
			closeConnection();
		} catch(Exception e){
			log.info(e);
		}
		
		
	}


	public void setTable(Table table) {
		this.table = table;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	private void closeConnection() throws Exception {
	
		connection.close();
	
	}

	private void buildConnection() throws Exception {
	
		Class.forName("com.mysql.jdbc.Driver");
		connection = DriverManager.getConnection(jdbcUrl, username, password);
		dc = DataContextFactory.createJdbcDataContext(connection);
	
	}

	

}
