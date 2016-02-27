package org.aksw.es.bsbmloader.metamodell;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.HashMap;
import java.util.Iterator;

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
public class NoSQLLoader implements Runnable {
	private UpdateableDataContext dc;
	private static org.apache.log4j.Logger log = Logger.getLogger(NoSQLLoader.class);
	private String schemaName;
	protected BlockingQueue<Row> queue = null;
	private Table table;
	private Column[] column;

	public void setQueue(BlockingQueue<Row> queue, Table table, Column[] column) {
		// super();
		this.table = table;
		this.column = column;
		this.queue = queue;
	}

	public NoSQLLoader() {
		super();
		// TODO Auto-generated constructor stub
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public void materializeSimpleData(String target, String source, String forgeinKey, String primaryKey) {
		Schema schema = dc.getSchemaByName(schemaName);
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

		DataSet dsJoin = dc.query().from(joinTable).select(fKey).where(secondFkey).eq(getInteger(id)).execute();

		while (dsJoin.next()) {
			for (SelectItem column : dsJoin.getSelectItems()) {
				list.add(dsJoin.getRow().getValue(column));
			}
		}

		dsJoin.close();
		Iterator<Object> liter = list.iterator();
		ArrayList<Map<String, Object>> test = new ArrayList<Map<String, Object>>();
		while (liter.hasNext()) {
			int i = getInteger(liter.next());

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

	public void createTable(Table table, Column[] column, ArrayList<String> fkColumn) {
		dc.executeUpdate(new UpdateScript() {
			private Table table;
			private Column[] column;
			ArrayList<String> fkColumn = new ArrayList<String>();

			public void run(UpdateCallback callback) {
				log.info("Create Schema");
				TableCreationBuilder tableCreation = callback.createTable(dc.getDefaultSchema(), table.getName());
				for (Column columnTable : column) {
					tableCreation.withColumn(columnTable.getName());
				}
				if (fkColumn != null) {
					for (String column : fkColumn) {
						tableCreation.withColumn(column);
					}
				}

				tableCreation.execute();
			}

			private UpdateScript init(Table table, Column[] column, ArrayList<String> fkColumn) {
				this.table = table;
				this.column = column;
				this.fkColumn = fkColumn;
				return this;
			}

		}.init(table, column, fkColumn));

		

	}

	public void insertRows(Table table, Column[] column, ArrayList<Row> rows) {
		dc.executeUpdate(new UpdateScript() {
			private Table table;
			private Column[] columns;
			private ArrayList<Row> rows = new ArrayList<Row>();

			public void run(UpdateCallback callback) {

				if (dc.getTableByQualifiedLabel(table.getName()) == null) {

				}

				Table tables = dc.getTableByQualifiedLabel(table.getName());

				RowInsertionBuilder rowsInsert = callback.insertInto(tables);
				for (Row insertRow : rows) {
					for (Column columnInsert : columns) {
						Object value = null;
						if (columnInsert.getType().isTimeBased()) {
							value = getDate(insertRow.getValue(columnInsert));
						} else {
							value = insertRow.getValue(columnInsert);
						}
						rowsInsert.value(columnInsert.getName(), value);

					}
					rowsInsert.execute();

				}

			}

			private UpdateScript init(Table table, Column[] column, ArrayList<Row> rows) {
				this.columns = column;
				this.table = table;
				this.rows = rows;
				return this;
			}

		}.init(table, column, rows));
	}

	private java.util.Date getDate(Object obj) {
		Date time = null;
		if (obj instanceof java.sql.Date) {
			time = new Date(((java.sql.Date) obj).getTime());
		}

		if (obj instanceof java.sql.Timestamp) {
			time = new Date(((Timestamp) obj).getTime());
		}

		return time;
	}

	private int getInteger(Object obj) {
		int i = java.lang.Integer.parseInt(obj.toString());
		return i;
	}

	public void run() {

		try {
			Row row;
			while ((row = queue.take()) != null) {
				// A Posion Row to kil the thread
				if (row.size() < 0) {
					return;
				}

				if (row.size() > 0) {
					dc.executeUpdate(new UpdateScript() {
						private Table table;
						private Column[] columns;
						protected Row queue = null;

						public void run(UpdateCallback callback) {

							Table tables = dc.getTableByQualifiedLabel(table.getName());
							RowInsertionBuilder rowsInsert = callback.insertInto(tables);

							for (Column columnInsert : columns) {
								Object value = null;
								if (columnInsert.getType().isTimeBased()) {
									value = getDate(queue.getValue(columnInsert));
								} else {
									value = queue.getValue(columnInsert);
								}
								rowsInsert.value(columnInsert.getName(), value);

							}
							rowsInsert.execute();

						}

						private UpdateScript init(Table table, Column[] column, Row queue) {
							this.columns = column;
							this.table = table;
							this.queue = queue;
							return this;
						}
					}.init(table, column, row));
				}

			}
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
	}

}
