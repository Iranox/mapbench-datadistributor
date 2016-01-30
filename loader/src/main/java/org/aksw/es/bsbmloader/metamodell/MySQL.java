package org.aksw.es.bsbmloader.metamodell;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.aksw.es.bsbmloader.tabledata.TableDataForgeinKey;
import org.aksw.es.bsbmloader.tabledata.TableDataPrimary;
import org.apache.log4j.Logger;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.DataContextFactory;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class MySQL {
	private Connection connection;
	private String jdbcUrl;
	private String username;
	private String password;
	private DataContext dc;
	private static org.apache.log4j.Logger log = Logger.getLogger(MySQL.class);
	
	
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
	
	public ArrayList<Row> getRowsMysql(Table table, String database) throws Exception{
		buildConnection();
		Schema schema = dc.getSchemaByName(database);
		Table tables = schema.getTableByName(table.getName());
		DataSet ds = dc.query().from(tables).selectAll().execute();
		ArrayList<Row> rows = new ArrayList<Row>();
		while(ds.next()){
			rows.add(ds.getRow());
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

	private void buildConnection() throws Exception {

		Class.forName("com.mysql.jdbc.Driver");
		connection = DriverManager.getConnection(jdbcUrl, username, password);
		dc = DataContextFactory.createJdbcDataContext(connection);

	}

	private void closeConnection() throws Exception {

		connection.close();

	}

	

}
