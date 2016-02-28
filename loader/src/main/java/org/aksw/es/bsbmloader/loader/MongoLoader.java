package org.aksw.es.bsbmloader.loader;


import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import org.aksw.es.bsbmloader.parser.ElementParser;
import org.apache.log4j.Logger;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.Update;

/**
 * @author Tobias
 */
public class MongoLoader  {
	private UpdateableDataContext dc;
	private static org.apache.log4j.Logger log = Logger.getLogger(MongoLoader.class);
	private String schemaName;


	public MongoLoader() {
		super();
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public void materializeSimpleData(String target, String source, String forgeinKey, String primaryKey) {
		Schema schema = dc.getSchemaByName(schemaName);
		log.info(schema);
		Column forgeinColumn = schema.getTableByName(target).getColumnByName(forgeinKey);
		Column primaryColumn = schema.getTableByName(source).getColumnByName(primaryKey);
		Column[] sourceColumns = schema.getTableByName(source).getColumns();
		Table targetTable = schema.getTableByName(target);
		Map<String, Object> nestedObj = new HashMap<String, Object>();
		DataSet ds = dc.query().from(source).selectAll().execute();

		while (ds.next()) {
			Object pk = ds.getRow().getValue(primaryColumn);
			for (Column columns : sourceColumns) {
				if (ds.getRow().getValue(columns) != null) {
					nestedObj.put(columns.getName(), ds.getRow().getValue(columns));
				}
			}
			dc.executeUpdate(new Update(targetTable).where(forgeinColumn).eq(pk).value(forgeinColumn, nestedObj));
			log.info("Test");
		}
		ds.close();

	}

	public void materializeComplexData(String database, String sourceTable, String fkJoinTable, String joinTable,
			String secondSourceTable, String pkSecondSource, String pkFirstSource, String secondFkey) {
		Schema schema = dc.getSchemaByName(database);
		Table tables = schema.getTableByName(sourceTable + "_mat");
		if (tables == null) {
			/**
			 * Create Table productfeatureproduct_mat
			 **/
			dc.executeUpdate(new UpdateScript() {
				private String sourceTable;
				private String database;
				private String fkJoinTable;

				public void run(UpdateCallback callback) {
					TableCreationBuilder tableCreation = callback.createTable(dc.getDefaultSchema(),
							sourceTable + "_mat");
					Schema schema = dc.getSchemaByName(database);
					tableCreation.withColumn(fkJoinTable);

					for (Column column : schema.getTableByName(sourceTable).getColumns()) {
						tableCreation.withColumn(column.getName());
					}

					tableCreation.execute();

				}

				private UpdateScript init(String sourceTable, String database, String fkJoinTable) {
					this.sourceTable = sourceTable;
					this.database = database;
					this.fkJoinTable = fkJoinTable;
					return this;
				}
			}.init(sourceTable, database, fkJoinTable));

			/**
			 * Insert Rows
			 */

			tables = schema.getTableByName(sourceTable);
			DataSet ds = dc.query().from(tables).selectAll().execute(); // get
																		// all
																		// Product
																		// Data
			while (ds.next()) {

				insertRows(tables, tables.getColumns(), ds.getRow(), fkJoinTable, secondSourceTable, joinTable,
						pkSecondSource, pkFirstSource, secondFkey);
			}
			ds.close();
		}
	}

	private void insertRows(Table table, Column[] column, Row row, String forgeinKey, String secondSourceTable,
			String joinTable, String pkSecondSource, String pkFirstSource, String secondFkey) {
		dc.executeUpdate(new UpdateScript() {
			private Table table;
			private Column[] column;
			private Row row;
			private String forgeinKey;
			private String secondSourceTable;
			private String joinTable;
			private String pkSecondSource;
			private String pkFirstSource;
			private String secondFkey;

			public void run(UpdateCallback callback) {
				Table tables = dc.getTableByQualifiedLabel(table.getName());
				Table matTable = dc.getTableByQualifiedLabel(table.getName() + "_mat");
				RowInsertionBuilder rowsInsert = callback.insertInto(matTable);
				for (Column columnInsert : column) {
					rowsInsert.value(columnInsert.getName(), row.getValue(columnInsert.getColumnNumber()));

				}
				Table table = dc.getTableByQualifiedLabel(secondSourceTable);
				Table join = dc.getTableByQualifiedLabel(joinTable);

				rowsInsert.value(forgeinKey,
						getComplexData(join, table, forgeinKey, pkSecondSource,
								row.getValue(tables.getColumnByName(pkFirstSource)), secondFkey, secondSourceTable,
								pkSecondSource).toArray());
				rowsInsert.execute();

			}

			private UpdateScript init(Table table, Column[] column, Row row, String name, String secondSourceTable,
					String joinTable, String pkSecondSource, String pkFirstSource, String secondFkey) {
				this.row = row;
				this.table = table;
				this.column = column;
				this.forgeinKey = name;
				this.secondSourceTable = secondSourceTable;
				this.joinTable = joinTable;
				this.pkSecondSource = pkSecondSource;
				this.pkFirstSource = pkFirstSource;
				this.secondFkey = secondFkey;
				return this;
			}
		}.init(table, column, row, forgeinKey, secondSourceTable, joinTable, pkSecondSource, pkFirstSource,
				secondFkey));

	}

	private ArrayList<Map<String, Object>> getComplexData(Table joinTable, Table table, String fKey, String jKey,
			Object id, String secondFkey, String secondSourceTable, String pkSecondSource) {
		ArrayList<Object> list = new ArrayList<Object>();

		DataSet dsJoin = dc.query().from(joinTable).select(fKey).where(secondFkey).eq(new ElementParser().getInteger(id)).execute();

		while (dsJoin.next()) {
			for (SelectItem column : dsJoin.getSelectItems()) {
				list.add(dsJoin.getRow().getValue(column));
			}
		}

		dsJoin.close();
		Iterator<Object> liter = list.iterator();
		ArrayList<Map<String, Object>> test = new ArrayList<Map<String, Object>>();
		while (liter.hasNext()) {
			int i = new ElementParser().getInteger(liter.next());

			DataSet dsTable = dc.query().from(secondSourceTable).selectAll().where(pkSecondSource).eq(i).execute();
			while (dsTable.next()) {
				Map<String, Object> nestedObj = new HashMap<String, Object>();
				for (SelectItem column : dsTable.getSelectItems()) {
					nestedObj.put(column.getColumn().getName(), dsTable.getRow().getValue(column));
				}
				test.add(nestedObj);
			}
		}

		return test;
	}

	public void deleteDatabase() {
		dc.executeUpdate(new UpdateScript() {

			public void run(UpdateCallback callback) {
				Schema schema = dc.getSchemaByName(schemaName);
				for (Table table : schema.getTables()) {
					if (!table.getName().contains("system")) {
						callback.dropTable(table).execute();
					}

				}

			}
		});
	}

	public void setUpdateableDataContext(UpdateableDataContext dc) throws Exception {
		this.dc = dc;
	}


	

	

	

}