package org.aksw.es.bsbmloader.importer;

import java.util.ArrayList;

import org.aksw.es.bsbmloader.connectionbuilder.ConnectionCreator;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.schema.Table;

public class Import {
	private UpdateableDataContext datacontextSource = null;
	private UpdateableDataContext firstDatacontextTarget = null;
	private String targetKey;
	private final int BORDER = 1000;
	private String databaseName;
	private Table target;
	private String primary;
	private ArrayList<UpdateableDataContext> targetDataContext = null;
	private int numberOfThreads = 3;

	public ArrayList<UpdateableDataContext> getTargetDataContext() {
		return targetDataContext;
	}

	public void setThreadsNumber(int numberOfThreads) {
		this.numberOfThreads = numberOfThreads;
	}

	public int getThreadsNumber() {
		return numberOfThreads;
	}

	public void setPrimary(String primary) {
		this.primary = primary;
	}

	public void setForgeinKey(String primaryKey) {
		this.targetKey = primaryKey;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	/**
	 * Connection to source database
	 * 
	 * @param url
	 * @param user
	 * @param password
	 * @param type
	 * @throws Exception
	 */
	public void createDataContext(String url, String user, String password, String type) throws Exception {
		datacontextSource = new ConnectionCreator().createNoSQLConnection(url, user, password, databaseName, type);
	}

	/**
	 * Connection to a jdbc database
	 * 
	 * @param url
	 * @param user
	 * @param password
	 * @throws Exception
	 */
	public void createDataContext(String url, String user, String password) throws Exception {
		datacontextSource = new ConnectionCreator().createJDBCConnection(url, user, password);
	}

	public void setDataContext(UpdateableDataContext datacontext) {
		this.datacontextSource = datacontext;
	}

	/**
	 * Create three datacontexts with the target database
	 * 
	 * @param url
	 * @param user
	 * @param password
	 * @param type
	 * @throws Exception
	 */

	public void createDataContextTarget(String url, String user, String password, String type) throws Exception {
		targetDataContext = new ArrayList<UpdateableDataContext>();
		for (int i = 0; i < numberOfThreads; i++) {
			targetDataContext
					.add(new ConnectionCreator().createNoSQLConnection(url, user, password, databaseName, type));
		}
	}

	public void setTargetTable(String tableName) {
		target = firstDatacontextTarget.getTableByQualifiedLabel(tableName);
	}

	public UpdateableDataContext getDatacontextSource() {
		return datacontextSource;
	}

	public UpdateableDataContext getFirstDatacontextTarget() {
		return targetDataContext.get(0);
	}

	public String getTargetKey() {
		return targetKey;
	}

	public int getBORDER() {
		return BORDER;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public Table getTarget() {
		return target;
	}

	public String getPrimary() {
		return primary;
	}

}
