package org.aksw.es.bsbmloader.starter;

import org.aksw.es.bsbmloader.connectionproperties.MongoConnectionProperties;
import org.aksw.es.bsbmloader.nosqlloader.NoSQLLoader;
import org.aksw.es.bsbmloader.nosqlloader.ComplexTableUpdater;
import org.aksw.es.bsbmloader.parser.NoSQLParser;
import org.apache.commons.cli.CommandLine;

public class MongoStarter implements Starter {
 
	//TODO 	restructuring function 
	public void startMaterializeSimple(CommandLine commandLine) throws Exception {
		if (commandLine.hasOption("target") && commandLine.hasOption("source")) {
			if (commandLine.hasOption("fk") && commandLine.hasOption("fk")) {
				MongoConnectionProperties mongo = new MongoConnectionProperties();
				mongo.setConnectionProperties(commandLine.getOptionValue("hostNosql"),
						commandLine.getOptionValue("portNosql"));
				NoSQLLoader mongoLoader = new NoSQLLoader();
				if (commandLine.hasOption("objectId")) {
					mongoLoader.setOnlyID(true);
				}

				if (commandLine.hasOption("databaseName")) {
					mongoLoader.setUpdateableDataContext(
							mongo.getDBwriteConcern(commandLine.getOptionValue("databaseName")));
				} else {
					throw new Exception("Missing parameter databaseName");
				}
				mongoLoader.materializeSimpleData(commandLine.getOptionValue("target"),
						commandLine.getOptionValue("source"), commandLine.getOptionValue("fk"),
						commandLine.getOptionValue("pk"));

			}

		}
	}

	//TODO 	restructuring function 
	public void startMaterializeComplex(CommandLine commandLine) throws Exception {
		ComplexTableUpdater test = new ComplexTableUpdater();
		if (commandLine.hasOption("join")) {
			MongoConnectionProperties mongo = new MongoConnectionProperties();
			mongo.setConnectionProperties(commandLine.getOptionValue("hostNosql"),
					commandLine.getOptionValue("portNosql"));

			NoSQLLoader mongoLoader = new NoSQLLoader();
			if (commandLine.hasOption("objectId")) {
				mongoLoader.setOnlyID(true);
			}
			if (commandLine.hasOption("databaseName")) {
				 
				 test.setDataContext(mongo.getDBwriteConcern(commandLine.getOptionValue("databaseName")));
				mongoLoader
						.setUpdateableDataContext(mongo.getDBwriteConcern(commandLine.getOptionValue("databaseName")));
			} else {
				throw new Exception("Missing parameter databaseName");
			}
			test.setPkSecond(commandLine.getOptionValue("pkSecond"));
			test.setSecondSource( commandLine.getOptionValue("secondSource"));
			
		test.compressData(commandLine.getOptionValue("join"), commandLine.getOptionValue("secondFkey"), commandLine.getOptionValue("fk"));
		mongoLoader.materializeSimpleData(commandLine.getOptionValue("join"),
				commandLine.getOptionValue("source"), commandLine.getOptionValue("secondFkey"),
				commandLine.getOptionValue("pk"));
		

		/**	mongoLoader.materializeComplexData(commandLine.getOptionValue("databaseName"),
					commandLine.getOptionValue("source"), commandLine.getOptionValue("fk"),
					commandLine.getOptionValue("join"), commandLine.getOptionValue("secondSource"),
					commandLine.getOptionValue("pkSecond"), commandLine.getOptionValue("pk"),
					commandLine.getOptionValue("secondFkey"));
			;
**/
		} 

	}
	//TODO 	restructuring function 
	public NoSQLParser createConnectionProperties(CommandLine commandLine) throws Exception {
		MongoConnectionProperties mongo = new MongoConnectionProperties();
		NoSQLParser nosql = new NoSQLParser();
		mongo.setConnectionProperties(commandLine.getOptionValue("hostNosql"), commandLine.getOptionValue("portNosql"));
		if (commandLine.hasOption("hostNosql") && commandLine.hasOption("portNosql")) {

			if (commandLine.hasOption("databaseName")) {
				nosql.setUpdateableDataContext(mongo.getDB(commandLine.getOptionValue("databaseName")));
			} else {
				throw new Exception("Missing parameter databaseName");
			}
		}
		
		return nosql;
	}

}
