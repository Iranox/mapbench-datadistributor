package org.aksw.es.bsbmloader.nosqlloader;

import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;

/**
 * @author Tobias
 */
public class NoSQLLoader {
	private UpdateableDataContext dataContext;
	private static final int BORDER_DATASET = 3000;
	private static final int INCREASE_OFFSET = 1000;
	private boolean onlyID = false;

	public NoSQLLoader() {
		super();
	}
	
	

	public void setOnlyID(boolean onlyID) {
		this.onlyID = onlyID;
	}



	//TODO 	restructuring function 
	public void materializeSimpleData(String target, String source, String forgeinKey, String primaryKey)
			throws Exception {
		int offset = 0;

		
		Column forgeinColumn = dataContext.getTableByQualifiedLabel(target).getColumnByName(forgeinKey);
		Column primaryColumn = dataContext.getTableByQualifiedLabel(source).getColumnByName(primaryKey);
		Column[] sourceColumns = dataContext.getTableByQualifiedLabel(source).getColumns();
		Table targetTable = dataContext.getTableByQualifiedLabel(target);

		int rowCount = new TableCounter().getRowNumber(dataContext, source);
		int startRows = rowCount;
		if (rowCount < 3000) {
			SimpleTableUpdater updater = new SimpleTableUpdater(dataContext, source);
			if(onlyID){
				updater.setOnlyID(true);
			}
			updater.setConnection(forgeinColumn, primaryColumn, targetTable, sourceColumns);
			updater.setLimit(rowCount);
			updater.updateData();
		} else {
			while (rowCount >= BORDER_DATASET) {

				SimpleTableUpdater firstUpdateThread = new SimpleTableUpdater(dataContext, source);
				firstUpdateThread.setConnection(forgeinColumn, primaryColumn, targetTable, sourceColumns);
				SimpleTableUpdater secondUpdateThread = new SimpleTableUpdater(dataContext, source);
				secondUpdateThread.setConnection(forgeinColumn, primaryColumn, targetTable, sourceColumns);
				SimpleTableUpdater thirdUpdateThread = new SimpleTableUpdater(dataContext, source);
				if(onlyID){
					firstUpdateThread.setOnlyID(true);
					secondUpdateThread.setOnlyID(true);
					thirdUpdateThread.setOnlyID(true);
				}
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
				
                //TODO Use Thread.join instead Thread.isAlive
				while (firstUpdateThread.isAlive() || secondUpdateThread.isAlive() || thirdUpdateThread.isAlive()) {
					Thread.sleep(1);
				}
			}
		}

		if (rowCount < 0) {
			SimpleTableUpdater updater = new SimpleTableUpdater(dataContext, source);
			if(onlyID){
				updater.setOnlyID(true);
			}
			updater.setDataContext(dataContext);
			updater.setLimit(rowCount);
			updater.setOffset(startRows - rowCount);
			updater.updateData();
		}
	}
	//TODO 	Still Need ?
	public void copyTable(String sourceTable, String targetTable) throws Exception {
		int offset = 0;
		int rowCount = new TableCounter().getRowNumber(dataContext, sourceTable);
		if (rowCount < 3000) {
			TableCopier tableCopier = new TableCopier(sourceTable, targetTable, dataContext);
			tableCopier.setLimit(rowCount);
			tableCopier.setOffset(0);
			tableCopier.start();
			rowCount = 0;
		} else {
			while (rowCount >= BORDER_DATASET) {

		
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
				 //TODO Use Thread.join instead Thread.isAlive
				while (firstTableCopierThread.isAlive() || secondTableCopierThread.isAlive()
						|| thirdTableCopierThread.isAlive()) {
					Thread.sleep(1);
				}
			}
		}

		if (rowCount > 0) {
			TableCopier tableCopier = new TableCopier(sourceTable, targetTable, dataContext);
			tableCopier.setLimit(rowCount);
			tableCopier.setOffset(0);
			tableCopier.start();
		}

	}

	public void createTable(String target, String forgeinKey) throws Exception {
		dataContext.executeUpdate(new UpdateScript() {
			private String targetTable;
			private String forgeinKey;

			public void run(UpdateCallback callback) {
				TableCreationBuilder tableCreation = callback.createTable(dataContext.getDefaultSchema(),
						targetTable + "_mat");

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
		copyTable(target, target + "_mat");

	}

	
    //TODO use new function. 
	public void materializeComplexData(String database, String sourceTable, String fkJoinTable, String joinTable,
			String secondSourceTable, String pkSecondSource, String pkFirstSource, String secondFkey) throws Exception {
		
		Table tables = dataContext.getTableByQualifiedLabel(sourceTable + "_matComplex");
		if (tables == null) {
			/**
			 * Create Table productfeatureproduct_mat
			 **/
			ComplexTableUpdater createTable = new ComplexTableUpdater(database, sourceTable, fkJoinTable, joinTable, secondSourceTable, pkSecondSource, pkFirstSource, secondFkey);
			createTable.setDataContext(dataContext);
			createTable.createComplexTable(sourceTable, database, fkJoinTable);
			
			/**
			 * Insert Rows
			 */
			int offset = 0;
			int rowCount = new TableCounter().getRowNumber(dataContext, sourceTable);
			int startRows = rowCount;
			if (rowCount < BORDER_DATASET) {
				ComplexTableUpdater updater = new ComplexTableUpdater(database, sourceTable, fkJoinTable, joinTable,
						secondSourceTable, pkSecondSource, pkFirstSource, secondFkey);
				if(onlyID){
					updater.setOnlyID(true);
				}
				updater.setDataContext(dataContext);
				updater.setLimit(rowCount);
				updater.materializeComplexData(database, sourceTable, fkJoinTable, joinTable, secondSourceTable,
						pkSecondSource, pkFirstSource, secondFkey);
			} else {
				while (rowCount >= BORDER_DATASET) {

					ComplexTableUpdater firstUpdateThread = new ComplexTableUpdater(database, sourceTable, fkJoinTable,
							joinTable, secondSourceTable, pkSecondSource, pkFirstSource, secondFkey);
					ComplexTableUpdater secondUpdateThread = new ComplexTableUpdater(database, sourceTable, fkJoinTable,
							joinTable, secondSourceTable, pkSecondSource, pkFirstSource, secondFkey);
					ComplexTableUpdater thirdUpdateThread = new ComplexTableUpdater(database, sourceTable, fkJoinTable,
							joinTable, secondSourceTable, pkSecondSource, pkFirstSource, secondFkey);
					firstUpdateThread.setDataContext(dataContext);
					secondUpdateThread.setDataContext(dataContext);
					thirdUpdateThread.setDataContext(dataContext);
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
					        
					//TODO Use Thread.join instead of Thread.isAlive
					while (firstUpdateThread.isAlive() || secondUpdateThread.isAlive() || thirdUpdateThread.isAlive()) {
						Thread.sleep(1);
					}
				}
			}

			if (rowCount < 0) {
				ComplexTableUpdater updater = new ComplexTableUpdater(database, sourceTable, fkJoinTable, joinTable,
						secondSourceTable, pkSecondSource, pkFirstSource, secondFkey);
				if(onlyID){
					updater.setOnlyID(true);
				}
				updater.setDataContext(dataContext);
				updater.setLimit(rowCount);
				updater.setOffset(startRows - rowCount);
				updater.materializeComplexData(database, sourceTable, fkJoinTable, joinTable, secondSourceTable,
						pkSecondSource, pkFirstSource, secondFkey);
			}
		}

	}

	


	public void setUpdateableDataContext(UpdateableDataContext dc) throws Exception {
		this.dataContext = dc;
	}

}
