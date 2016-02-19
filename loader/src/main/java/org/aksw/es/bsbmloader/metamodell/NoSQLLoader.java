package org.aksw.es.bsbmloader.metamodell;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.Update;

/**
 * @author Tobias
 */
public class NoSQLLoader {
	private UpdateableDataContext dc;
	private static org.apache.log4j.Logger log = Logger.getLogger(NoSQLLoader.class);
	private String schemaName;

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
		}
		ds.close();

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

	public void createTable(Table table, Column[] column) {
		dc.executeUpdate(new UpdateScript() {
			private Table table;
			private Column[] column;

			public void run(UpdateCallback callback) {
				log.info("Create Schema");
				TableCreationBuilder tableCreation = callback.createTable(dc.getDefaultSchema(), table.getName());
				for (Column columnTable : column) {
					tableCreation.withColumn(columnTable.getName());
				}
				tableCreation.execute();
			}

			private UpdateScript init(Table table, Column[] column) {
				this.table = table;
				this.column = column;
				return this;
			}

		}.init(table, column));

	}

	public void insertRows(Table table, Column[] column, ArrayList<Row> rows) {
		dc.executeUpdate(new UpdateScript() {
			private Table table;
			private Column[] columns;
			private ArrayList<Row> rows = new ArrayList<Row>();

			public void run(UpdateCallback callback) {

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

}
