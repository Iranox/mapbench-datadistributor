package org.aksw.es.bsbmloader.nosqlloader;


/**
 * @author Tobias
 */
public class NoSQLLoader {
/**	private UpdateableDataContext dataContext;
	private static final int BORDER_DATASET = 3000;
	private static final int INCREASE_OFFSET = 1000;
	private boolean onlyID = false;
	private Column forgeinColumn;
	private Column primaryColumn;
	private Column[] sourceColumns;
	private Table targetTable;
	private int offset;
	private int rowCount;
	private int startRows;

	public NoSQLLoader() {
		super();
		offset = 0;
	}

	public void setOnlyID(boolean onlyID) {
		this.onlyID = onlyID;
	}
	

	public void materializeSimpleData(String target, String source, String forgeinKey, String primaryKey)
			throws Exception {

		forgeinColumn = dataContext.getTableByQualifiedLabel(target).getColumnByName(forgeinKey);
		primaryColumn = dataContext.getTableByQualifiedLabel(source).getColumnByName(primaryKey);
		sourceColumns = dataContext.getTableByQualifiedLabel(source).getColumns();
		targetTable = dataContext.getTableByQualifiedLabel(target);

	    rowCount = new TableCounter().getRowNumber(dataContext, source);
		startRows = rowCount;
		if (rowCount < BORDER_DATASET ) {
			simpleUpdateWithoutThread(source);
		} else {
			 simpleUpdateThreads(source);
		}

	
		if (rowCount < 0) {
			simpleUpdateWithoutTHreadRemain(source);
		}
	}


	public void materializeComplexData(String database, String sourceTable, String fkJoinTable, String joinTable,
			String secondSourceTable, String pkSecondSource, String pkFirstSource, String secondFkey) throws Exception {

		ComplexTableUpdater createTable = new ComplexTableUpdater();
		createTable.setDataContext(dataContext);

	
		int rowCount = new TableCounter().getRowNumber(dataContext, sourceTable);
		int startRows = rowCount;
		if (rowCount < BORDER_DATASET) {
			ComplexTableUpdater updater = new ComplexTableUpdater();
			updater.setPkSecond(pkSecondSource);
			updater.setSecondSource(secondSourceTable);
			
			updater.setDataContext(dataContext);
			updater.setLimit(rowCount);
			updater.compressData(joinTable, secondFkey, fkJoinTable);
		} else {
			while (rowCount >= BORDER_DATASET) {

				ComplexTableUpdater firstUpdateThread = new ComplexTableUpdater();
				ComplexTableUpdater secondUpdateThread = new ComplexTableUpdater();
				ComplexTableUpdater thirdUpdateThread = new ComplexTableUpdater();
				firstUpdateThread.setPkSecond(pkSecondSource);
				firstUpdateThread.setSecondSource(secondSourceTable);
				firstUpdateThread.setDataContext(dataContext);
				secondUpdateThread.setPkSecond(pkSecondSource);
				secondUpdateThread.setSecondSource(secondSourceTable);
				thirdUpdateThread.setPkSecond(pkSecondSource);
				thirdUpdateThread.setSecondSource(secondSourceTable);
				firstUpdateThread.setProperty(joinTable, secondFkey, fkJoinTable);
				secondUpdateThread.setProperty(joinTable, secondFkey, fkJoinTable);
				thirdUpdateThread.setProperty(joinTable, secondFkey, fkJoinTable);
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

				firstUpdateThread.join();
				secondUpdateThread.join();
				thirdUpdateThread.join();
			}
		}

		if (rowCount < 0) {
			ComplexTableUpdater updater = new ComplexTableUpdater();
			updater.setPkSecond(pkSecondSource);
			updater.setSecondSource(secondSourceTable);
			/**
			 * if(onlyID){ updater.setOnlyID(true); }
		
			updater.setDataContext(dataContext);
			updater.setLimit(startRows - rowCount);
			updater.compressData(joinTable, secondFkey, fkJoinTable);

		}

	}

	public void setUpdateableDataContext(UpdateableDataContext dc) throws Exception {
		this.dataContext = dc;
	}
	
	private void simpleUpdateWithoutTHreadRemain(String source) {

		SimpleTableUpdater updater = new SimpleTableUpdater(dataContext, source);
		if (onlyID) {
			updater.setOnlyID(true);
		}
		updater.setDataContext(dataContext);
		updater.setLimit(rowCount);
		updater.setOffset(startRows - rowCount);
		updater.updateData();

	}
	
	private void simpleUpdateWithoutThread(String source) throws Exception {
		int rowCount = new TableCounter().getRowNumber(dataContext, source);
		SimpleTableUpdater updater = new SimpleTableUpdater(dataContext, source);
		if (onlyID) {
			updater.setOnlyID(true);
		}
		updater.setConnection(forgeinColumn, primaryColumn, targetTable, sourceColumns);
		updater.setLimit(rowCount);
		updater.updateData();
	}
	
	private SimpleTableUpdater setSimpleThreadProperty(String source){
		SimpleTableUpdater updateThread = new SimpleTableUpdater(dataContext, source);
		updateThread.setConnection(forgeinColumn, primaryColumn, targetTable, sourceColumns);
		if(onlyID){
			updateThread.setOnlyID(true);
		}
		return updateThread;
	}
	
	private void changeOffsetAndLimit(){
		offset += INCREASE_OFFSET;
		rowCount -= INCREASE_OFFSET;
	}

	
	private void simpleUpdateThreads(String source) throws Exception{
		while (rowCount >= BORDER_DATASET) {

			SimpleTableUpdater firstUpdateThread = setSimpleThreadProperty(source);
			SimpleTableUpdater secondUpdateThread =setSimpleThreadProperty(source);
			SimpleTableUpdater thirdUpdateThread = setSimpleThreadProperty(source);
	
			firstUpdateThread.setOffset(offset);
		    changeOffsetAndLimit();
			secondUpdateThread.setOffset(offset);
			changeOffsetAndLimit();
			thirdUpdateThread.setOffset(offset);
			changeOffsetAndLimit();
			firstUpdateThread.start();
			secondUpdateThread.start();
			thirdUpdateThread.start();

			firstUpdateThread.join();
			secondUpdateThread.join();
			thirdUpdateThread.join();
		}
	}
**/


}
