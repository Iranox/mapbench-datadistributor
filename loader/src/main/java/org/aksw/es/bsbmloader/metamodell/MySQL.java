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

	public void getComplexRelation(String tableName) throws Exception {
		buildConnection();
		Schema schema = dc.getSchemaByName("benchmark");
		Table table = schema.getTableByName(tableName);
		for (Relationship relation : table.getRelationships()) {
			if (isJoinTable(relation.getForeignTable())) {
				getJoinData(relation.getForeignTable(), relation.getPrimaryTable());
			}
		}
	}

	/**
	 * Is this table a joinTable
	 * 
	 * @param table
	 * @return
	 */
	private boolean isJoinTable(Table table) throws Exception {
		buildConnection();
		if (table.getForeignKeys().length == table.getPrimaryKeys().length) {
			return true;
		}
		closeConnection();
		return false;
	}

	/**
	 * Get Data of JoinTable
	 * 
	 * @param table
	 * @param primaryTable
	 */
	public void getJoinData(Table table, Table primaryTable) throws Exception {
		buildConnection();
		for (Relationship relation : table.getForeignKeyRelationships()) {
			if (!relation.getPrimaryTable().getName().equals(primaryTable.getName())) {
				DataSet ds = dc.query().from(relation.getPrimaryTable().getName()).innerJoin(table)
						.on(relation.getPrimaryColumns()[0], relation.getForeignColumns()[0]).selectAll().execute();
				log.info(ds.toRows().size());
			}

		}

	}

	/**
	 * get all Join Tables
	 * 
	 * @return
	 */
	public ArrayList<Table> getComplexTable() throws Exception {
		buildConnection();
		HashMap<String, ArrayList<Row>> tmp = new HashMap<String, ArrayList<Row>>();

		Schema schema = dc.getSchemaByName("benchmark");

		Table[] tables = schema.getTables();
		ArrayList<Table> complexTable = new ArrayList<Table>();
		for (Table table : tables) {
			if (table.getForeignKeys().length == table.getPrimaryKeys().length && table.getForeignKeys().length > 1) {
				complexTable.add(table);
			}
		}
		closeConnection();
		log.info(complexTable.size());
		return complexTable;

	}

	/**
	 * Get the tables of a Relationship
	 * 
	 * @param table
	 * @return
	 */
	public ArrayList<Table> getKeyPrimaryTable(Table table) throws Exception {
		buildConnection();
		ArrayList<Table> primaryTable = new ArrayList<Table>();
		for (Relationship relation : table.getForeignKeyRelationships()) {
			primaryTable.add(relation.getPrimaryTable());
			log.info(relation.getPrimaryTable().getName());
		}
		closeConnection();
		return primaryTable;
	}

	/**
	 * get Primarykeys of a Table
	 * 
	 * @return
	 */
	public ArrayList<Row> getPrimaryKeyValue() throws Exception {
		buildConnection();
		Schema schema = dc.getSchemaByName("benchmark");
		Table table = schema.getTableByName("product");
		DataSet set = dc.query().from(table).select(table.getPrimaryKeys()).execute();
		ArrayList<Row> rows = new ArrayList<Row>();
		while (set.next()) {
			rows.add(set.getRow());
		}
		set.close();
		closeConnection();
		return rows;

	}

	public ArrayList<TableDataPrimary> readData() throws Exception {
		log.info("Read Data form MySQL");
		buildConnection();
		ArrayList<TableDataPrimary> tableData = new ArrayList<TableDataPrimary>();
		Schema schema = dc.getSchemaByName("benchmark");
		schema.getTableByName("benchmark");
		Table[] tables = schema.getTables();
		for (Table table : tables) {
			TableDataPrimary tmp = new TableDataPrimary();
			ArrayList<TableDataForgeinKey> fkData = new ArrayList<TableDataForgeinKey>();
			tmp.setTable(table);
			tmp.setRows(getRows(table.getName()));
			for (Relationship relationship : table.getForeignKeyRelationships()) {
				TableDataForgeinKey fkTmp = new TableDataForgeinKey();
				tmp.insertRelationship(relationship.getForeignColumns()[0].getName(),
						relationship.getPrimaryTable().getName()); // Column
																	// PrimaryTable
				fkTmp.setRows(
						getForgeinData(relationship.getPrimaryTable().getName(), relationship.getPrimaryColumns()[0])); // Daten
																														// der
																														// Primary
																														// Table
																														// einfügen
				fkTmp.setTable(relationship.getPrimaryTable());
				tmp.insertFkTable(relationship.getPrimaryTable().getName().toString(), fkTmp);

			}
			tableData.add(tmp);
		}

		closeConnection();
		log.info("Read Data form MySQL -- Done");
		return tableData;

	}

	public void setConnectionProperties(String jdbcUrl, String username, String password) throws Exception {
		this.jdbcUrl = jdbcUrl;
		this.username = username;
		this.password = password;
	}

	private HashMap<String, Row> getForgeinData(String table, Column primaryKey) throws Exception {
		HashMap<String, Row> rows = new HashMap<String, Row>();
		DataSet ds = dc.query().from("benchmark." + table).selectAll().execute();
		while (ds.next()) {
			rows.put(ds.getRow().getValue(primaryKey).toString(), ds.getRow());
		}
		ds.close();

		return rows;

	}

	private void buildConnection() throws Exception {

		Class.forName("com.mysql.jdbc.Driver");
		connection = DriverManager.getConnection(jdbcUrl, username, password);
		dc = DataContextFactory.createJdbcDataContext(connection);

	}

	private void closeConnection() throws Exception {

		connection.close();

	}

	private ArrayList<Row> getRows(String tableName) throws Exception {
		ArrayList<Row> rows = new ArrayList<Row>();
		DataSet ds = dc.query().from("benchmark." + tableName).selectAll().execute();
		while (ds.next()) {
			rows.add(ds.getRow());
		}
		ds.close();
		return rows;
	}

}
