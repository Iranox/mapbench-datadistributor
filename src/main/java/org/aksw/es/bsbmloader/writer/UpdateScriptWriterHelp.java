package org.aksw.es.bsbmloader.writer;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;

public class UpdateScriptWriterHelp {
	private final static int NUMBER = 3000;

	private UpdateScriptWriterHelp() {

	}

	public static UpdateScript updateScriptVertical(final DataContext dataContext, final Table table,
			final String[] columnsName, final String id, final String type) {
		if (type == null) {
			return updateScriptVertical(dataContext, table, columnsName, id);
		}
		return null;

	}

	public static UpdateScript createInsertScript(final DataContext dataContext, final Table table,
			final IQueryRewriter typ) {
		if (typ == null) {
			return updateScript(dataContext, table);
		}

		return updateScriptWithIQueryRewriter(dataContext, table, typ);
	}

	private static UpdateScript updateScript(final DataContext dataContext, final Table table) {

		UpdateScript updateScript = new UpdateScript() {

			private TableCreationBuilder tableCreation = null;

			public void run(UpdateCallback callback) {

				tableCreation = callback.createTable(dataContext.getDefaultSchema(), table.getName());
				for (Column column : table.getColumns()) {
					if (column.getType().isLiteral()) {
						tableCreation.withColumn(column.getName()).ofType(column.getType());
					} else {
						tableCreation.withColumn(column.getName()).ofType(column.getType());
					}
				}
				tableCreation.execute();

			}// run function

		};
		return updateScript;
	}

	private static UpdateScript updateScriptVertical(final DataContext dataContext, final Table table,
			final String[] columnsName, final String id) {

		UpdateScript updateScript = new UpdateScript() {

			private TableCreationBuilder tableCreation = null;

			public void run(UpdateCallback callback) {

				tableCreation = callback.createTable(dataContext.getDefaultSchema(), table.getName());
				tableCreation.withColumn(id).ofType(table.getColumnByName(id).getType());
				for (String column : columnsName) {
					tableCreation.withColumn(column).ofType(table.getColumnByName(id).getType());

				}
				tableCreation.execute();

			}// run function

		};
		return updateScript;
	}

	private static UpdateScript updateScriptWithIQueryRewriter(final DataContext dataContext, final Table table,
			final IQueryRewriter typ) {

		UpdateScript updateScript = new UpdateScript() {

			public void run(UpdateCallback callback) {
				

				TableCreationBuilder tableCreation = callback.createTable(dataContext.getDefaultSchema(),
						table.getName());
				for (Column column : table.getColumns()) {
					if (column.getType().isLiteral()) {
						if (column.getColumnSize() < NUMBER) {
							tableCreation.withColumn(column.getName())
									.ofNativeType(typ.rewriteColumnType(column.getType(), column.getColumnSize()));
						} else {
							tableCreation.withColumn(column.getName())
									.ofNativeType(typ.rewriteColumnType(ColumnType.STRING, null)).nullable(true);
						}

					} else {
						tableCreation.withColumn(column.getName()).nullable(true)
								.ofNativeType(typ.rewriteColumnType(column.getType(), null));

					}

				}
				tableCreation.execute();

			}

		};

		return updateScript;
	}

}
