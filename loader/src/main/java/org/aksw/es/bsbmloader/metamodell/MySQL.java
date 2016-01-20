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



	public ArrayList<TableDataPrimary> test() {
		log.info("Read Data form MySQL");
		buildConnection();
		ArrayList<TableDataPrimary> tableData = new ArrayList<TableDataPrimary>();
		Schema schema = dc.getSchemaByName("benchmark");
		schema.getTableByName("benchmark");
		Table[] tables = schema.getTables();
		for(Table table : tables){
			TableDataPrimary tmp = new TableDataPrimary();
			ArrayList<TableDataForgeinKey> fkData = new ArrayList<TableDataForgeinKey>();
			tmp.setTable(table);
			tmp.setRows(getRows(table.getName()));
			for(Relationship relationship : table.getForeignKeyRelationships()){
				TableDataForgeinKey fkTmp = new TableDataForgeinKey();
				tmp.insertRelationship(relationship.getForeignColumns()[0].getName(), relationship.getPrimaryTable().getName()); // Column  PrimaryTable
				fkTmp.setRows(getForgeinData(relationship.getPrimaryTable().getName(), relationship.getPrimaryColumns()[0])); // Daten der Primary Table einfügen
				fkTmp.setTable(relationship.getPrimaryTable());
				tmp.insertFkTable(relationship.getPrimaryTable().getName().toString(), fkTmp);

			}
			tableData.add(tmp);
		}

		closeConnection();
		log.info("Read Data form MySQL -- Done");
		return tableData;

	}
	
	public void setConnectionProperties(String jdbcUrl, String username, String password) {
		this.jdbcUrl = jdbcUrl;
		this.username = username;
		this.password = password;
	}

	private HashMap<String, Row> getForgeinData(String table, Column primaryKey){
		HashMap<String, Row> rows = new HashMap<String, Row>();
		DataSet ds = dc.query().from("benchmark." + table).selectAll().execute();
		while(ds.next()){
			rows.put(ds.getRow().getValue(primaryKey).toString(), ds.getRow());
		}
		ds.close();
		
		return rows;
	
	}

	private void buildConnection() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(jdbcUrl, username, password);
			dc = DataContextFactory.createJdbcDataContext(connection);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private void closeConnection() {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	private ArrayList<Row> getRows(String tableName) {
		ArrayList<Row> rows = new ArrayList<Row>();
		DataSet ds = dc.query().from("benchmark." + tableName).selectAll().execute();
		while (ds.next()) {
			rows.add(ds.getRow());
		}
		ds.close();
		return rows;
	}

}
