package org.aksw.es.bsbmloader.connectionbuilder;

import org.apache.metamodel.UpdateableDataContext;

public interface ConnectionDatabase {
	public void setDatabaseName(String name);
	public UpdateableDataContext getDB() throws Exception;
	public void setConnectionProperties(String jdbcurl, String user, String password) throws Exception;
	public void setConnectionProperties(String hostname, String port) throws Exception;

}
