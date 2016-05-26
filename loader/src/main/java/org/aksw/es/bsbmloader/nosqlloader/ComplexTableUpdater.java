package org.aksw.es.bsbmloader.nosqlloader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.aksw.es.bsbmloader.parser.ElementParser;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;

//TODO Remove Class, Use instead Test 
public class ComplexTableUpdater extends Thread {
	private UpdateableDataContext dataContext;
	private int limit;
	private int offset;
	private String database;
	private String sourceTable;
	private String fkJoinTable;
	private String joinTable;
	private String secondSourceTable;
	private String pkSecondSource;
	private String pkFirstSource;
	private String secondFkey;
	private boolean onlyID = false;

	public ComplexTableUpdater(String database, String sourceTable, String fkJoinTable, String joinTable,
			String secondSourceTable, String pkSecondSource, String pkFirstSource, String secondFkey) {
		super();
		this.database = database;
		this.sourceTable = sourceTable;
		this.fkJoinTable = fkJoinTable;
		this.joinTable = joinTable;
		this.secondSourceTable = secondSourceTable;
		this.pkSecondSource = pkSecondSource;
		this.pkFirstSource = pkFirstSource;
		this.secondFkey = secondFkey;
	}

	public void setOnlyID(boolean onlyID) {
		this.onlyID = onlyID;
	}

	public void setDataContext(UpdateableDataContext dataContext) {
		this.dataContext = dataContext;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public void createComplexTable(String sourceTable, String database, String fkJoinTable) {
		dataContext.executeUpdate(new UpdateScript() {
			private String sourceTable;
			private String fkJoinTable;

			public void run(UpdateCallback callback) {
				TableCreationBuilder tableCreation = callback.createTable(dataContext.getDefaultSchema(),
						sourceTable + "_matComplex");
				tableCreation.withColumn(fkJoinTable).ofType(ColumnType.ARRAY);

				for (Column column : dataContext.getTableByQualifiedLabel(sourceTable).getColumns()) {
					tableCreation.withColumn(column.getName());
				}

				tableCreation.execute();

			}

			private UpdateScript init(String sourceTable,  String fkJoinTable) {
				this.sourceTable = sourceTable;
				this.fkJoinTable = fkJoinTable;
				return this;
			}
		}.init(sourceTable, fkJoinTable));
	}

	public void materializeComplexData(String database, String sourceTable, String fkJoinTable, String joinTable,
			String secondSourceTable, String pkSecondSource, String pkFirstSource, String secondFkey) {
		Table tables = dataContext.getTableByQualifiedLabel(sourceTable + "_matComplex");

		tables = dataContext.getTableByQualifiedLabel(sourceTable);
		DataSet dataSet = dataContext.query().from(tables).selectAll().limit(limit).offset(offset).execute();// get
		// all
		// Product
		// Data
		while (dataSet.next()) {

			insertRows(tables, tables.getColumns(), dataSet.getRow(), fkJoinTable, secondSourceTable, joinTable,
					pkSecondSource, pkFirstSource, secondFkey);
		}
		dataSet.close();

	}

	public void insertRows(Table table, Column[] column, Row row, String forgeinKey, String secondSourceTable,
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
				if (!onlyID) {
					Map<String, Object> nestedObj = new HashMap<String, Object>();
					for (SelectItem column : dataSetTable.getSelectItems()) {
						nestedObj.put(column.getColumn().getName(), dataSetTable.getRow().getValue(column));
					}
					complexData.add(nestedObj);
				} else {
					Map<String, Object> nestedObj = new HashMap<String, Object>();
					for (SelectItem column : dataSetTable.getSelectItems()) {
						if (column.getColumn().isPrimaryKey()) {
							nestedObj.put(column.getColumn().getName(), dataSetTable.getRow().getValue(column));
						}

					}
					complexData.add(nestedObj);
				}

			}
			dataSetTable.close();
		}

		return complexData;
	}

	public void run() {
		materializeComplexData(database, sourceTable, fkJoinTable, joinTable, secondSourceTable, pkSecondSource,
				pkFirstSource, secondFkey);
	}

}
