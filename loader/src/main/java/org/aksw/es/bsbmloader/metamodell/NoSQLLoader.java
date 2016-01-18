package org.aksw.es.bsbmloader.metamodell;

import java.util.ArrayList;

import org.aksw.es.bsbmloader.tabledata.TableData;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

/**
 * @author Tobias
 */
public class NoSQLLoader {
	private UpdateableDataContext dc;

	public void insertSimpleTable(UpdateableDataContext da, ArrayList<TableData> tableData) {
		dc = da;
		dc.executeUpdate(new UpdateScript() {
			private ArrayList<TableData> tableData;
			private Schema defaultSchema;

			public void run(UpdateCallback callback) {
				for (int index = 0; index < tableData.size(); index++) {
					TableCreationBuilder tableCreation = callback.createTable(defaultSchema,
							tableData.get(index).getTable());

					for (Column column : tableData.get(index).getColumns()) {

						tableCreation.withColumn(column.getName().replace("nr", "_id"));
					}
					Table table = tableCreation.execute();

					RowInsertionBuilder rows = callback.insertInto(table);
					for (Row row : tableData.get(index).getRows()) {
						for (Column column : tableData.get(index).getColumns()) {
							if (row.getValue(column.getColumnNumber()) == null)
								rows.value(table.getColumn(column.getColumnNumber()), "null");
							else
								rows.value(table.getColumn(column.getColumnNumber()),
										row.getValue(column.getColumnNumber()).toString());

						}
						rows.execute();
					}

				}
			}

			private UpdateScript init(ArrayList<TableData> tableData, Schema defaultSchema) {
				this.tableData = tableData;
				this.defaultSchema = defaultSchema;
				return this;
			}
		}.init(tableData, dc.getDefaultSchema()));
	}

}
