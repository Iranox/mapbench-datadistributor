package org.aksw.es.bsbmloader.writer;

import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.drop.DropTable;
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;

public class TableCreator {
	private UpdateableDataContext dataContext;
	private Table tables[];
	private final int NUMBER = 3000;

	public void setUpdateableDataContext(UpdateableDataContext dc) throws Exception {
		this.dataContext = dc;
	}

	public void createTable(Table table[], final IQueryRewriter typ) {
		this.tables = table;
		UpdateScript script = null;
		if (typ == null) {
			script = updateScript();
		} else {
			script = updateScriptWithIQueryRewriter(typ);
		}
		dataContext.executeUpdate(script);

	}

	public void deleteDatabase(Table table) {
		if (dataContext.getTableByQualifiedLabel(table.getName()) != null) {
			dataContext.executeUpdate(new DropTable(table.getName()));
		}
	}

	private UpdateScript updateScript() {

		UpdateScript updateScript = new UpdateScript() {

			public void run(UpdateCallback callback) {
				for (Table table : tables) {
					TableCreationBuilder tableCreation = callback.createTable(dataContext.getDefaultSchema(),
							table.getName());
					for (Column column : table.getColumns()) {
						if (column.getType().isLiteral()) {
							tableCreation.withColumn(column.getName()).ofType(column.getType());
						} else {
							tableCreation.withColumn(column.getName()).ofType(column.getType());
						}
					}
					tableCreation.execute();
				}

			}// run function
		};
		return updateScript;
	}

	private UpdateScript updateScriptWithIQueryRewriter(final IQueryRewriter typ) {

		UpdateScript updateScript = new UpdateScript() {

			public void run(UpdateCallback callback) {
				for (Table table : tables) {
					TableCreationBuilder tableCreation = callback.createTable(dataContext.getDefaultSchema(),
							table.getName());
					for (Column column : table.getColumns()) {
						if (column.getType().isLiteral()) {
							if (column.getColumnSize() < NUMBER) {
								tableCreation.withColumn(column.getName())
										.ofNativeType(typ.rewriteColumnType(column.getType(), column.getColumnSize()));
							} else {
								tableCreation.withColumn(column.getName())
										.ofNativeType(typ.rewriteColumnType(ColumnType.STRING, null));
							}
						} else {
							tableCreation.withColumn(column.getName())
									.ofNativeType(typ.rewriteColumnType(column.getType(), null));

						}
					}
					tableCreation.execute();
				}

			}

		};

		return updateScript;
	}

}
