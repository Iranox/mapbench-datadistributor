package org.aksw.es.bsbmloader.starter;


import org.aksw.es.bsbmloader.reader.DataReader;
import org.apache.commons.cli.CommandLine;

public class CouchStarter{
	
	
	/**public void startMaterializeSimple(CommandLine commandLine) throws Exception {
		if (commandLine.hasOption("target") && commandLine.hasOption("source") && commandLine.hasOption("fk")
				&& commandLine.hasOption("fk")) {
			CouchConnectionProperties couch = new CouchConnectionProperties();
			couch.setConnectionProperties(commandLine.getOptionValue("hostNosql"),
					commandLine.getOptionValue("portNosql"));
			NoSQLLoader couchLoader = new NoSQLLoader();
			if (commandLine.hasOption("databaseName")) {
				couchLoader.setUpdateableDataContext(couch.getDB(commandLine.getOptionValue("userCouch"),
						commandLine.getOptionValue("passwordCouch")));
			} else {
				throw new Exception("Missing parameter databaseName");
			}
			couchLoader.materializeSimpleData(commandLine.getOptionValue("target"), commandLine.getOptionValue("source"),
					commandLine.getOptionValue("fk"), commandLine.getOptionValue("pk"));

		}

	}**/


	/**public  void startMaterializeComplex(CommandLine commandLine) throws Exception {
		if (commandLine.hasOption("join")) {
			CouchConnectionProperties couch = new CouchConnectionProperties();
			couch.setConnectionProperties(commandLine.getOptionValue("hostNosql"),
					commandLine.getOptionValue("portNosql"));
			NoSQLLoader couchLoader = new NoSQLLoader();
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

	}**/


	/**public NoSQLParser createConnectionProperties(CommandLine commandLine) throws Exception {
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
	}**/



}
