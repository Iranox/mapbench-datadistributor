package org.aksw.es.bsbmloader.connectionbuilder;



import org.apache.commons.cli.CommandLine;
import org.apache.metamodel.UpdateableDataContext;


public class ConnectionCreator {
	
	public UpdateableDataContext createConnection(CommandLine commandLine, String type) throws Exception {
		ConnectionDatabase connection = null;
		if (type != null && type.equals("mysql")) {
			connection = new ConnectionBuilder().createConnectionProperties("mysql");
			connection.setConnectionProperties(commandLine.getOptionValue("urlMysql"), commandLine.getOptionValue("u"),
					commandLine.getOptionValue("p"));
			return connection.getDB();
		}
		
		if (commandLine.hasOption("materializeMongo") || commandLine.hasOption("parseToMongo")) {
			connection = new ConnectionBuilder().createConnectionProperties("mongodb");
			connection.setConnectionProperties(commandLine.getOptionValue("hostNosql"),commandLine.getOptionValue("portNosql"));
			connection.setDatabaseName(commandLine.getOptionValue("databaseName"));
				
			return connection.getDB();
		}

		return null;

	}
	 
}
