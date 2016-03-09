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
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.Update;

/**
 * @author Tobias
 */
public class MongoLoader  {
	private UpdateableDataContext dataContext;
	private String schemaName;


	public MongoLoader() {
		super();
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public void materializeSimpleData(String target, String source, String forgeinKey, String primaryKey) {
		createTable(target, forgeinKey);
		Schema schema = dataContext.getSchemaByName(schemaName);
		Column forgeinColumn = schema.getTableByName(target+ "_mat").getColumnByName(forgeinKey);
		Column primaryColumn = schema.getTableByName(source).getColumnByName(primaryKey);
		Column[] sourceColumns = schema.getTableByName(source).getColumns();
		Table targetTable = schema.getTableByName(target + "_mat");
		Map<String, Object> nestedObj = new HashMap<String, Object>();
		DataSet dataSet = dataContext.query().from(source).selectAll().execute();
		

		while (dataSet.next()) {
			Object primaryKeyObject = dataSet.getRow().getValue(primaryColumn);
			for (Column columns : sourceColumns) {
				if (dataSet.getRow().getValue(columns) != null) {
					nestedObj.put(columns.getName(), dataSet.getRow().getValue(columns));
				}
			}
			dataContext.executeUpdate(new Update(targetTable).where(forgeinColumn).eq(primaryKeyObject).value(forgeinColumn, nestedObj));
			
		}
		dataSet.close();

	}
	
	public void copyTable(String sourceTable, String targetTable){
		dataContext.executeUpdate(new UpdateScript() {
			
			private String sourceTable;
			private String targetTable;
			
			public void run(UpdateCallback callback) {
				Table table =dataContext.getTableByQualifiedLabel(targetTable);
				DataSet dataSet = dataContext.query().from(sourceTable).selectAll().execute();
				RowInsertionBuilder rowsInsert = callback.insertInto(table);
				while(dataSet.next()){
					Row row = dataSet.getRow();
					for (Column columnInsert : table.getColumns()) {
						rowsInsert.value(columnInsert.getName(), row.getValue(columnInsert.getColumnNumber()));

					}
					rowsInsert.execute();
				}
				
				
			}
			
			private UpdateScript init(String sourceTable, String targetTable){
				this.sourceTable = sourceTable;
				this.targetTable = targetTable;
				return this;
				
			}
		}.init(sourceTable, targetTable));
		
		
		
	}
	
	public void createTable(String target, String forgeinKey){
		dataContext.executeUpdate(new UpdateScript() {
			private String targetTable;
			private String forgeinKey;


			public void run(UpdateCallback callback) {
				TableCreationBuilder tableCreation = callback.createTable(dataContext.getDefaultSchema(),
						targetTable + "_mat");
			
				
				for (Column column : dataContext.getTableByQualifiedLabel(targetTable).getColumns()) {
					if(!column.equals(forgeinKey)){
						tableCreation.withColumn(column.getName()).ofType(column.getType());
					}
					else{
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
		copyTable(target, target + "_mat");

		
	}
	
	private void createComplexTable(String sourceTable, String database, String fkJoinTable){
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

	public void materializeComplexData(String database, String sourceTable, String fkJoinTable, String joinTable,
			String secondSourceTable, String pkSecondSource, String pkFirstSource, String secondFkey) {
		Schema schema = dataContext.getSchemaByName(database);
		Table tables = schema.getTableByName(sourceTable + "_matComplex" );
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

		DataSet dataSetJoin = dataContext.query().from(joinTable).select(fKey).where(secondFkey).eq(new ElementParser().getInteger(id)).execute();

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

			DataSet dataSetTable = dataContext.query().from(secondSourceTable).selectAll().where(pkSecondSource).eq(index).execute();
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

	
	public void deleteDatabase() {
		dataContext.executeUpdate(new UpdateScript() {

			public void run(UpdateCallback callback) {
				Schema schema = dataContext.getSchemaByName(schemaName);
				for (Table table : schema.getTables()) {
					if (!table.getName().contains("system")) {
						callback.dropTable(table).execute();
					}

				}

			}
		});
	}

	public void setUpdateableDataContext(UpdateableDataContext dc) throws Exception {
		this.dataContext = dc;
	}

}
