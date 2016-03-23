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

/**
 * @author Tobias
 */
public class MongoLoader  {
	private UpdateableDataContext dataContext;
	private String schemaName;
	private static final int BORDER_DATASET = 3000;
	private static final int INCREASE_OFFSET = 1000;
	


	public MongoLoader() {
		super();
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
	
	

	public void materializeSimpleData(String target, String source, String forgeinKey, String primaryKey) throws Exception {
		int offset = 0;
		
		
		Schema schema = dataContext.getSchemaByName(schemaName);
		Column forgeinColumn = schema.getTableByName(target).getColumnByName(forgeinKey);
		Column primaryColumn = schema.getTableByName(source).getColumnByName(primaryKey);
		Column[] sourceColumns = schema.getTableByName(source).getColumns();
		Table targetTable = schema.getTableByName(target);
	
		int rowCount = new TableCounter().getRowNumber(dataContext, source);
		int startRows = rowCount;
		if(rowCount < 3000){
			NoSQLUpdater updater = new NoSQLUpdater(dataContext, source);
			updater.setConnection(forgeinColumn, primaryColumn, targetTable, sourceColumns);
			updater.setLimit(rowCount);
			updater.updateData();
		}
		else{
			while(rowCount >= BORDER_DATASET){
				
				NoSQLUpdater firstUpdateThread =  new NoSQLUpdater(dataContext, source);
				firstUpdateThread.setConnection(forgeinColumn, primaryColumn, targetTable, sourceColumns);
				NoSQLUpdater secondUpdateThread = new NoSQLUpdater(dataContext, source);
				secondUpdateThread.setConnection(forgeinColumn, primaryColumn, targetTable, sourceColumns);
				NoSQLUpdater thirdUpdateThread =   new NoSQLUpdater(dataContext, source);
				thirdUpdateThread.setConnection(forgeinColumn, primaryColumn, targetTable, sourceColumns);
				firstUpdateThread.setOffset(offset);
				offset += INCREASE_OFFSET;
				rowCount -= INCREASE_OFFSET;
				secondUpdateThread.setOffset(offset);
				offset += INCREASE_OFFSET;
				rowCount -= INCREASE_OFFSET;
				thirdUpdateThread.setOffset(offset);
				offset += INCREASE_OFFSET;
				rowCount -= INCREASE_OFFSET;
				firstUpdateThread.start();
				secondUpdateThread.start();
				thirdUpdateThread.start();
				
				while(firstUpdateThread.isAlive() || secondUpdateThread.isAlive() || thirdUpdateThread.isAlive()){
					Thread.sleep(1);
				}
			}
		}
		
		if(rowCount < 0){
			NoSQLUpdater updater = new NoSQLUpdater(dataContext, source);
			updater.setDataContext(dataContext);
			updater.setLimit(rowCount);
			updater.setOffset(startRows - rowCount);
			updater.updateData();
		}
	}
	
	public void copyTable(String sourceTable, String targetTable) throws Exception{
		int offset = 0;
		int rowCount = new TableCounter().getRowNumber(dataContext, sourceTable);
		if(rowCount < 3000){
			TableCopier tableCopier = new TableCopier(sourceTable, targetTable, dataContext);
			tableCopier.setLimit(rowCount);
			tableCopier.setOffset(0);
			tableCopier.start();
			rowCount = 0;
		}
		else{
			while(rowCount >= BORDER_DATASET){
				
				System.out.println(offset);
				TableCopier firstTableCopierThread = new TableCopier(sourceTable, targetTable, dataContext);
				TableCopier secondTableCopierThread = new TableCopier(sourceTable, targetTable, dataContext);
				TableCopier thirdTableCopierThread = new TableCopier(sourceTable, targetTable, dataContext);
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
				
				while(firstTableCopierThread.isAlive() || secondTableCopierThread.isAlive() || thirdTableCopierThread.isAlive()){
					Thread.sleep(1);
				}
			}
		}
		
		if(rowCount > 0){
			TableCopier tableCopier = new TableCopier(sourceTable, targetTable, dataContext);
			tableCopier.setLimit(rowCount);
			tableCopier.setOffset(0);
			tableCopier.start();
		}
		
		
	}
	
	public void createTable(String target, String forgeinKey) throws Exception{
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
