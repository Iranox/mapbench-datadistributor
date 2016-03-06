package org.aksw.es.bsbmloader.starter;

import org.aksw.es.bsbmloader.connectionproperties.ElasticConnectionProperties;
import org.aksw.es.bsbmloader.parser.NoSQLParser;
import org.apache.commons.cli.CommandLine;

public class ElasticSearchStarter implements Starter{

	public void startMaterializeComplex(CommandLine commandLine) throws Exception {
		// TODO Auto-generated method stub
		
	}

	public void startMaterializeSimple(CommandLine commandLine) throws Exception {
		// TODO Auto-generated method stub
		
	}

	public NoSQLParser createConnectionProperties(CommandLine commandLine) throws Exception {
		ElasticConnectionProperties elastic = new ElasticConnectionProperties();
		NoSQLParser nosql = new NoSQLParser();
		if (commandLine.hasOption("hostNosql")) {
			
			elastic.setConnectionProperties(commandLine.getOptionValue("hostNosql"));
			if (commandLine.hasOption("databaseName")) {
				nosql.setUpdateableDataContext(elastic.getDB(commandLine.getOptionValue("databaseName")));
			} else {
				throw new Exception("Missing parameter databaseName");
			}
		}
		return nosql;
	}

}
