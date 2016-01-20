package org.aksw.es.bsbmloader.metamodell;

import java.util.ArrayList;
import java.util.HashMap;


import org.aksw.es.bsbmloader.tabledata.TableDataPrimary;
import org.apache.log4j.Logger;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;

/**
 * @author Tobias
 */
public class NoSQLLoader {
	private UpdateableDataContext dc;
	private static org.apache.log4j.Logger log = Logger.getLogger(NoSQLLoader.class);


	public void insertData(UpdateableDataContext da, ArrayList<TableDataPrimary> tableData) {
		dc = da;
		dc.executeUpdate(new UpdateScript() {
			private ArrayList<TableDataPrimary> tableData;
			private Schema defaultSchema;

			public void run(UpdateCallback callback) {
				log.info("Create Schema");
				for (TableDataPrimary tableDataprim : tableData) {
					TableCreationBuilder tableCreation = callback.createTable(defaultSchema,
							tableDataprim.getTable().getName());
					for (Column column : tableDataprim.getTable().getColumns()) {
						tableCreation.withColumn(column.getName());
					}
					tableCreation.execute();
				}
				log.info("Create Schema -- Done");
				
				log.info("Insert into NoSQL Database");

				for (TableDataPrimary tableDataprim : tableData) {
					RowInsertionBuilder rows = callback.insertInto(tableDataprim.getTable().getName());
					for (Row row : tableDataprim.getRows()) {
						for (Column column : tableDataprim.getTable().getColumns()) {
							String fk = tableDataprim.getRelationshipTable(column.getName());
							if (fk == null) {
								if (row.getValue(column.getColumnNumber()) == null)
									rows.value(column.getName(), "null");
								else
									rows.value(column.getName(), row.getValue(column.getColumnNumber()).toString());

							} else {
								HashMap<String, Row> tmp = tableDataprim.getFKTable(fk).getRows();
								Row tmpRow = tmp.get(row.getValue(column.getColumnNumber()).toString());
								rows.value(column.getName(), tmpRow.getValues()[1]);

							}
						}
						rows.execute();
					}
				}
				log.info("Insert into NoSQL Database -- Done");

			}

			private UpdateScript init(ArrayList<TableDataPrimary> tableData, Schema defaultSchema) {
				this.tableData = tableData;
				this.defaultSchema = defaultSchema;
				return this;
			}
		}.init(tableData, dc.getDefaultSchema()));
	}

}
