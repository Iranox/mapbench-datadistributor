package org.aksw.es.bsbmloader.main;

import org.aksw.es.bsbmloader.connectionproperties.CouchConnectionProperties;
import org.aksw.es.bsbmloader.loader.CouchLoader;
import org.apache.commons.cli.CommandLine;

public class CouchStarter {
	public static void materializeSimpleTable(CommandLine commandLine) throws Exception {
		if (commandLine.hasOption("target") && commandLine.hasOption("source") && commandLine.hasOption("fk")
				&& commandLine.hasOption("fk")) {
			CouchConnectionProperties couch = new CouchConnectionProperties();
			couch.setConnectionProperties(commandLine.getOptionValue("hostNosql"),
					commandLine.getOptionValue("portNosql"));
			CouchLoader nosql = new CouchLoader();
			if (commandLine.hasOption("databaseName")) {
				nosql.setUpdateableDataContext(couch.getDB(commandLine.getOptionValue("userCouch"),
						commandLine.getOptionValue("passwordCouch")));
			} else {
				throw new Exception("Missing parameter databaseName");
			}
			nosql.materializeSimpleData(commandLine.getOptionValue("target"), commandLine.getOptionValue("source"),
					commandLine.getOptionValue("fk"), commandLine.getOptionValue("pk"));

		}

	}

	public static void materializeComplexTable(CommandLine commandLine) throws Exception {
		if (commandLine.hasOption("join")) {
			CouchConnectionProperties couch = new CouchConnectionProperties();
			couch.setConnectionProperties(commandLine.getOptionValue("hostNosql"),
					commandLine.getOptionValue("portNosql"));
			CouchLoader nosql = new CouchLoader();
			if (commandLine.hasOption("databaseName")) {
				nosql.setUpdateableDataContext(couch.getDB(commandLine.getOptionValue("userCouch"),
						commandLine.getOptionValue("passwordCouch")));
				
			} else {
				throw new Exception("Missing parameter databaseName");
			}

			nosql.materializeComplexData(commandLine.getOptionValue("databaseName"),
					commandLine.getOptionValue("source"), commandLine.getOptionValue("fk"),
					commandLine.getOptionValue("join"), commandLine.getOptionValue("secondSource"),
					commandLine.getOptionValue("pkSecond"), commandLine.getOptionValue("pk"),
					commandLine.getOptionValue("secondFkey"));
			;

		}

	}

}
