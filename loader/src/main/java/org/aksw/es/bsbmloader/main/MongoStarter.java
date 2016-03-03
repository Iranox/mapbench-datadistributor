package org.aksw.es.bsbmloader.main;

import org.aksw.es.bsbmloader.connectionproperties.MongoConnectionProperties;
import org.aksw.es.bsbmloader.loader.MongoLoader;
import org.apache.commons.cli.CommandLine;

public class MongoStarter {

	public static void materializeSimpleTable(CommandLine commandLine) throws Exception {
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
				mongoLoader.materializeSimpleData(commandLine.getOptionValue("target"), commandLine.getOptionValue("source"),
						commandLine.getOptionValue("fk"), commandLine.getOptionValue("pk"));

			}

		}
	}

	public static void materializeComplexTable(CommandLine commandLine) throws Exception {
		if (commandLine.hasOption("join") && commandLine.hasOption("join")) {
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

}
