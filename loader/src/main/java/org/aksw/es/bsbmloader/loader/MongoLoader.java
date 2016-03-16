package org.aksw.es.bsbmloader.loader;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import org.aksw.es.bsbmloader.parser.ElementParser;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.drop.DropTable;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

/**
 * @author Tobias
 */
public class MongoLoader {
	private UpdateableDataContext dataContext;
	private String schemaName;
	private static final int BORDER_DATASET = 3000;
	private static final int INCREASE_OFFSET = 1000;
	private static final String SUFFIX_NEW_TABLE = "_mat";

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
	
	public void deleteDatabase(String Name) {
		dataContext.executeUpdate(new DropTable(Name + SUFFIX_NEW_TABLE));
	}

	public void materializeSimpleData(String target, String source, String forgeinKey, String primaryKey)
			throws Exception {
		createTable(target, forgeinKey);
		materializeSimpleTable(source, target + SUFFIX_NEW_TABLE, forgeinKey, primaryKey);
	}

	public void materializeSimpleTable(String sourceTable, String targetTable, String fk, String pk) throws Exception {
		int offset = 0;
		int rowCount = new TableCounter().getRowNumber(dataContext, targetTable.replace(SUFFIX_NEW_TABLE, ""));
		int startRowCount = rowCount;
		System.out.println(rowCount);
		if (rowCount < BORDER_DATASET) {
			SimpleTableUpdater tableCopier = new SimpleTableUpdater(dataContext, sourceTable, targetTable, fk, pk);
			tableCopier.setLimit(rowCount);
			tableCopier.setOffset(0);
			tableCopier.insertData();
			rowCount = 0;
		} else {
			while (rowCount >= BORDER_DATASET) {

				SimpleTableUpdater firstTableCopierThread = new SimpleTableUpdater(dataContext, sourceTable, targetTable, fk, pk);
				SimpleTableUpdater secondTableCopierThread = new SimpleTableUpdater(dataContext, sourceTable, targetTable, fk, pk);
				SimpleTableUpdater thirdTableCopierThread = new SimpleTableUpdater(dataContext, sourceTable, targetTable, fk, pk);

				firstTableCopierThread.setOffset(offset);
				offset += INCREASE_OFFSET;
				rowCount -= INCREASE_OFFSET;
				secondTableCopierThread.setOffset(offset);
				offset += INCREASE_OFFSET;
				rowCount -= INCREASE_OFFSET;
				thirdTableCopierThread.setOffset(offset);
				offset += INCREASE_OFFSET;
				rowCount -= INCREASE_OFFSET;

				firstTableCopierThread.start();
				secondTableCopierThread.start();
				thirdTableCopierThread.start();

				while (firstTableCopierThread.isAlive() || secondTableCopierThread.isAlive()
						|| thirdTableCopierThread.isAlive()) {
					Thread.sleep(1);
				}
			}
		}

		if (rowCount > 0) {
			SimpleTableUpdater tableCopier = new SimpleTableUpdater(sourceTable, targetTable, dataContext);
			tableCopier.setLimit(rowCount);
			tableCopier.setOffset(startRowCount - rowCount);
			tableCopier.insertData();
		}
	}

	public void createTable(String target, String forgeinKey) throws Exception {
		dataContext.executeUpdate(new UpdateScript() {
			private String targetTable;
			private String forgeinKey;

			public void run(UpdateCallback callback) {
				TableCreationBuilder tableCreation = callback.createTable(dataContext.getDefaultSchema(),
						targetTable + SUFFIX_NEW_TABLE);

				for (Column column : dataContext.getTableByQualifiedLabel(targetTable).getColumns()) {
					if (!column.equals(forgeinKey)) {
						tableCreation.withColumn(column.getName()).ofType(column.getType());
					} else {
						tableCreation.withColumn(column.getName()).ofType(ColumnType.MAP);
					}

				}

				tableCreation.execute();

			}

			private UpdateScript init(String targetTable, String forgeinKey) {
				this.forgeinKey = forgeinKey;
				this.targetTable = targetTable;
				return this;
			}
		}.init(target, forgeinKey));

	}

	public void materializeComplexData(String database, String sourceTable, String fkJoinTable, String joinTable,
			String secondSourceTable, String pkSecondSource, String pkFirstSource, String secondFkey) {
		Schema schema = dataContext.getSchemaByName(database);
		Table tables = schema.getTableByName(sourceTable + "_matComplex");
		if (tables == null) {
			/**
			 * Create Table productfeatureproduct_mat
			 **/
			createComplexTable(secondSourceTable, database, fkJoinTable);

			/**
			 * Insert Rows
			 */

			tables = schema.getTableByName(sourceTable);
			DataSet dataSet = dataContext.query().from(tables).selectAll().execute(); // get
			// all
			// Product
			// Data
			while (dataSet.next()) {

				insertRows(tables, tables.getColumns(), dataSet.getRow(), fkJoinTable, secondSourceTable, joinTable,
						pkSecondSource, pkFirstSource, secondFkey);
			}
			dataSet.close();
		}
	}

	public void setUpdateableDataContext(UpdateableDataContext dc) throws Exception {
		this.dataContext = dc;
	}

	private void insertRows(Table table, Column[] column, Row row, String forgeinKey, String secondSourceTable,
			String joinTable, String pkSecondSource, String pkFirstSource, String secondFkey) {
		dataContext.executeUpdate(new UpdateScript() {
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
				Table tables = dataContext.getTableByQualifiedLabel(table.getName());
				Table matTable = dataContext.getTableByQualifiedLabel(table.getName() + "_matComplex");
				RowInsertionBuilder rowsInsert = callback.insertInto(matTable);
				for (Column columnInsert : column) {
					rowsInsert.value(columnInsert.getName(), row.getValue(columnInsert.getColumnNumber()));

				}
				Table table = dataContext.getTableByQualifiedLabel(secondSourceTable);
				Table join = dataContext.getTableByQualifiedLabel(joinTable);

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
		ArrayList<Object> listRows = new ArrayList<Object>();

		DataSet dataSetJoin = dataContext.query().from(joinTable).select(fKey).where(secondFkey)
				.eq(new ElementParser().getInteger(id)).execute();

		while (dataSetJoin.next()) {
			for (SelectItem column : dataSetJoin.getSelectItems()) {
				listRows.add(dataSetJoin.getRow().getValue(column));
			}
		}

		dataSetJoin.close();
		Iterator<Object> listIter = listRows.iterator();
		ArrayList<Map<String, Object>> complexData = new ArrayList<Map<String, Object>>();
		while (listIter.hasNext()) {
			int index = new ElementParser().getInteger(listIter.next());

			DataSet dataSetTable = dataContext.query().from(secondSourceTable).selectAll().where(pkSecondSource)
					.eq(index).execute();
			while (dataSetTable.next()) {
				Map<String, Object> nestedObj = new HashMap<String, Object>();
				for (SelectItem column : dataSetTable.getSelectItems()) {
					nestedObj.put(column.getColumn().getName(), dataSetTable.getRow().getValue(column));
				}
				complexData.add(nestedObj);
			}
			dataSetTable.close();
		}

		return complexData;
	}

	private void createComplexTable(String sourceTable, String database, String fkJoinTable) {
		dataContext.executeUpdate(new UpdateScript() {
			private String sourceTable;
			private String database;
			private String fkJoinTable;

			public void run(UpdateCallback callback) {
				TableCreationBuilder tableCreation = callback.createTable(dataContext.getDefaultSchema(),
						sourceTable + "_matComplex");
				Schema schema = dataContext.getSchemaByName(database);
				tableCreation.withColumn(fkJoinTable).ofType(ColumnType.ARRAY);

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
	}

}
