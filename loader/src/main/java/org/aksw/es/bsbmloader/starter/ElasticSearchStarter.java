package org.aksw.es.bsbmloader.starter;

import org.aksw.es.bsbmloader.connectionproperties.ElasticConnectionProperties;
import org.aksw.es.bsbmloader.loader.MongoLoader;
import org.aksw.es.bsbmloader.parser.NoSQLParser;
import org.apache.commons.cli.CommandLine;

public class ElasticSearchStarter implements Starter{

	public void startMaterializeComplex(CommandLine commandLine) throws Exception {
		// TODO Implement Complex for elastic
		
	}

	public void startMaterializeSimple(CommandLine commandLine) throws Exception {
		if (commandLine.hasOption("target") && commandLine.hasOption("source")) {
			if (commandLine.hasOption("fk") && commandLine.hasOption("fk")) {
				ElasticConnectionProperties elastic = new ElasticConnectionProperties();
				elastic.setConnectionProperties(commandLine.getOptionValue("hostNosql"));
				MongoLoader nosqlLoader = new MongoLoader();
				if (commandLine.hasOption("databaseName")) {
					nosqlLoader.setUpdateableDataContext(elastic.getDB(commandLine.getOptionValue("databaseName")));
					nosqlLoader.setSchemaName(commandLine.getOptionValue("databaseName"));
				} else {
					throw new Exception("Missing parameter databaseName");
				}
				nosqlLoader.materializeSimpleData(commandLine.getOptionValue("target"), commandLine.getOptionValue("source"),
						commandLine.getOptionValue("fk"), commandLine.getOptionValue("pk"));

			}

		}
		
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
