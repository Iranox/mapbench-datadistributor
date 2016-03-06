package org.aksw.es.bsbmloader.starter;

import org.aksw.es.bsbmloader.connectionproperties.CouchConnectionProperties;
import org.aksw.es.bsbmloader.loader.CouchLoader;
import org.aksw.es.bsbmloader.parser.NoSQLParser;
import org.apache.commons.cli.CommandLine;

public class CouchStarter implements Starter{
	
	public void startMaterializeSimple(CommandLine commandLine) throws Exception {
		if (commandLine.hasOption("target") && commandLine.hasOption("source") && commandLine.hasOption("fk")
				&& commandLine.hasOption("fk")) {
			CouchConnectionProperties couch = new CouchConnectionProperties();
			couch.setConnectionProperties(commandLine.getOptionValue("hostNosql"),
					commandLine.getOptionValue("portNosql"));
			CouchLoader couchLoader = new CouchLoader();
			if (commandLine.hasOption("databaseName")) {
				couchLoader.setUpdateableDataContext(couch.getDB(commandLine.getOptionValue("userCouch"),
						commandLine.getOptionValue("passwordCouch")));
			} else {
				throw new Exception("Missing parameter databaseName");
			}
			couchLoader.materializeSimpleData(commandLine.getOptionValue("target"), commandLine.getOptionValue("source"),
					commandLine.getOptionValue("fk"), commandLine.getOptionValue("pk"));

		}

	}

	public  void startMaterializeComplex(CommandLine commandLine) throws Exception {
		if (commandLine.hasOption("join")) {
			CouchConnectionProperties couch = new CouchConnectionProperties();
			couch.setConnectionProperties(commandLine.getOptionValue("hostNosql"),
					commandLine.getOptionValue("portNosql"));
			CouchLoader couchLoader = new CouchLoader();
			if (commandLine.hasOption("databaseName")) {
				couchLoader.setUpdateableDataContext(couch.getDB(commandLine.getOptionValue("userCouch"),
						commandLine.getOptionValue("passwordCouch")));
				
			} else {
				throw new Exception("Missing parameter databaseName");
			}

			couchLoader.materializeComplexData(commandLine.getOptionValue("databaseName"),
					commandLine.getOptionValue("source"), commandLine.getOptionValue("fk"),
					commandLine.getOptionValue("join"), commandLine.getOptionValue("secondSource"),
					commandLine.getOptionValue("pkSecond"), commandLine.getOptionValue("pk"),
					commandLine.getOptionValue("secondFkey"));
			;

		}

	}




	public NoSQLParser createConnectionProperties(CommandLine commandLine) throws Exception {
		NoSQLParser nosql = new NoSQLParser();
		CouchConnectionProperties couch = new CouchConnectionProperties();
		couch.setConnectionProperties(commandLine.getOptionValue("hostNosql"),
				commandLine.getOptionValue("portNosql"));
		if (commandLine.hasOption("hostNosql") && commandLine.hasOption("portNosql")) {
			if (commandLine.hasOption("databaseName")) {
				nosql.setUpdateableDataContext(couch.getDB(commandLine.getOptionValue("userCouch"),
						commandLine.getOptionValue("passwordCouch")));
			} else {
				throw new Exception("Missing parameter databaseName");
			}
		}
		return nosql;
	}

}
