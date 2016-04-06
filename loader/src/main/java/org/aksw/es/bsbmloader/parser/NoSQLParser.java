package org.aksw.es.bsbmloader.parser;

import java.util.concurrent.BlockingQueue;

import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;

public class NoSQLParser implements Runnable {
	protected BlockingQueue<Row> queue = null;
	private Table table;
	private Column[] column;
	private UpdateableDataContext dataContext;
	private int rowCount = 0;

	public void setQueue(BlockingQueue<Row> queue, Table table, Column[] column) {
		this.table = table;
		this.column = column;
		this.queue = queue;
	}

	public UpdateableDataContext getDc() {
		return dataContext;
	}
	
	

	public int getRowCount() {
		return rowCount;
	}

	public void setUpdateableDataContext(UpdateableDataContext dc) throws Exception {
		this.dataContext = dc;
	}

	public void createTable(Table table, Column[] column, final IQueryRewriter typ) {
		dataContext.executeUpdate(new UpdateScript() {
			private Table table;
			private Column[] column;
			private TableCreationBuilder tableCreation;

			private void createMysqlTable(Column columnTable) {
				if (columnTable.getType().isLiteral() && columnTable.getColumnSize() < 30000) {
					tableCreation.withColumn(columnTable.getName())
							.ofNativeType(typ.rewriteColumnType(columnTable.getType(), columnTable.getColumnSize()));
				} else {
					if (columnTable.getType().isLiteral() && columnTable.getColumnSize() > 30000) {
						tableCreation.withColumn(columnTable.getName())
								.ofNativeType(typ.rewriteColumnType(ColumnType.STRING, null));
					} else {
						tableCreation.withColumn(columnTable.getName())
								.ofNativeType(typ.rewriteColumnType(columnTable.getType(), null));
					}
				}
			}

			public void run(UpdateCallback callback) {
				tableCreation = callback.createTable(dataContext.getDefaultSchema(), table.getName());
				for (Column columnTable : column) {

					if (typ == null) {
						if (columnTable.getType().isLiteral()) {
							tableCreation.withColumn(columnTable.getName()).ofType(columnTable.getType());
						} else {
							tableCreation.withColumn(columnTable.getName()).ofType(columnTable.getType());
						}
					} else {
						createMysqlTable(columnTable);
					}

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
	

	public void deleteDatabase(String name) {
		dataContext.executeUpdate(new UpdateScript() {
			private String name;

			public void run(UpdateCallback callback) {
			    Table table = dataContext.getTableByQualifiedLabel(name);
			    callback.dropTable(table).execute();;
			}
			
		private UpdateScript init(String name){
			this.name = name;
			return this;
		}
		}.init(name));
	}

	public void run() {
		

		try {
			Row row;
			while ((row = queue.take()) != null) {
				// A Posion Row to kill the thread
				if (row.size() < 0) {
					return;
				}

				if (row.size() > 0) {
					dataContext.executeUpdate(new UpdateScript() {
						private Table table;
						private Column[] columns;
						protected Row queue = null;

						public void run(UpdateCallback callback) {

							Table tables = dataContext.getTableByQualifiedLabel(table.getName());
							RowInsertionBuilder rowsInsert = callback.insertInto(tables);

							for (Column columnInsert : columns) {
								Object value = null;
								if (columnInsert.getType().isTimeBased()) {
									value = new ElementParser().getDate(queue.getValue(columnInsert));
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
					rowCount++;
				}

			}
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
	}

}
