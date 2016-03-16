package org.aksw.es.bsbmloader.starter;

import org.aksw.es.bsbmloader.connectionproperties.MongoConnectionProperties;
import org.aksw.es.bsbmloader.loader.MongoLoader;
import org.aksw.es.bsbmloader.parser.NoSQLParser;
import org.apache.commons.cli.CommandLine;

public class MongoStarter implements Starter {
	
	

	public void startMaterializeSimple(CommandLine commandLine) throws Exception {
		if (commandLine.hasOption("target") && commandLine.hasOption("source")) {
			if (commandLine.hasOption("fk") && commandLine.hasOption("fk")) {
				MongoConnectionProperties mongo = new MongoConnectionProperties();
				mongo.setConnectionProperties(commandLine.getOptionValue("hostNosql"),
						commandLine.getOptionValue("portNosql"));
				MongoLoader mongoLoader = new MongoLoader();
				if (commandLine.hasOption("databaseName")) {
					mongoLoader.setUpdateableDataContext(mongo.getDB(commandLine.getOptionValue("databaseName")));
					mongoLoader.setSchemaName(commandLine.getOptionValue("databaseName"));
				} else {
					throw new Exception("Missing parameter databaseName");
				}
				
				if (commandLine.hasOption("d")) {
					 mongoLoader.deleteDatabase(commandLine.getOptionValue("target"));
				}
				mongoLoader.materializeSimpleData(commandLine.getOptionValue("target"), commandLine.getOptionValue("source"),
						commandLine.getOptionValue("fk"), commandLine.getOptionValue("pk"));

			}

		}
	}

	public  void startMaterializeComplex(CommandLine commandLine) throws Exception {
		if (commandLine.hasOption("join")) {
			MongoConnectionProperties mongo = new MongoConnectionProperties();
			mongo.setConnectionProperties(commandLine.getOptionValue("hostNosql"),
					commandLine.getOptionValue("portNosql"));
			MongoLoader mongoLoader = new MongoLoader();
			if (commandLine.hasOption("databaseName")) {
				mongoLoader.setUpdateableDataContext(mongo.getDB(commandLine.getOptionValue("databaseName")));
				mongoLoader.setSchemaName(commandLine.getOptionValue("databaseName"));
			} else {
				throw new Exception("Missing parameter databaseName");
			}

			mongoLoader.materializeComplexData(commandLine.getOptionValue("databaseName"),
					commandLine.getOptionValue("source"), commandLine.getOptionValue("fk"),
					commandLine.getOptionValue("join"), commandLine.getOptionValue("secondSource"),
					commandLine.getOptionValue("pkSecond"), commandLine.getOptionValue("pk"),
					commandLine.getOptionValue("secondFkey"));
			;

		}

	}

	public NoSQLParser createConnectionProperties(CommandLine commandLine) throws Exception {
		MongoConnectionProperties mongo = new MongoConnectionProperties();
		NoSQLParser nosql = new NoSQLParser();
		mongo.setConnectionProperties(commandLine.getOptionValue("hostNosql"),
				commandLine.getOptionValue("portNosql"));
		if (commandLine.hasOption("hostNosql") && commandLine.hasOption("portNosql")) {
			
			if (commandLine.hasOption("databaseName")) {
				nosql.setUpdateableDataContext(mongo.getDB(commandLine.getOptionValue("databaseName")));
			} else {
				throw new Exception("Missing parameter databaseName");
			}
		}
		System.out.println(nosql.getDc());
		return nosql;
	}


	

}
