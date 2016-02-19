package org.aksw.es.bsbmloader.main;

import org.aksw.es.bsbmloader.loader.Database;
import org.aksw.es.bsbmloader.metamodell.MongoConnectionProperties;
import org.aksw.es.bsbmloader.metamodell.MySQL;
import org.aksw.es.bsbmloader.metamodell.NoSQLLoader;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.apache.commons.cli.CommandLine;

public class Main {
	private static org.apache.log4j.Logger log = Logger.getLogger(Main.class);
	
	public static void main(String[] args) {
		CommandLineParser parser = new BasicParser();
	
		try {
			CommandLine commandLine = parser.parse(getOption(), args);
			if (commandLine.hasOption("h")) {
				HelpFormatter formater = new HelpFormatter();
				formater.printHelp("Parameter", getOption());
			}
	
			if (commandLine.hasOption("importToMysql") && hasMySQLConnectionProperties(commandLine)) {
				if (!commandLine.hasOption("file")) {
					startIntDatabase(commandLine.getOptionValue("u"), commandLine.getOptionValue("p"),
							commandLine.getOptionValue("urlMysql"));
				} else {
					startIntDatabase(commandLine.getOptionValue("u"), commandLine.getOptionValue("p"),
							commandLine.getOptionValue("urlMysql"), commandLine.getOptionValue("file"));
				}
	
			}
	
			if (commandLine.hasOption("parseToMongo") && hasMySQLConnectionProperties(commandLine)) {
				if (commandLine.hasOption("hostMongo") && commandLine.hasOption("portMongo")) {
					startParseToMongoDB(commandLine);
	
				} else {
					log.info("The programm need username, password and jdbc-url for the mysqlServer! \n\t"
							+ "For more Inforamtion use -h or --help!");
				}
	
			}
			
			if (commandLine.hasOption("materializeMongo")) {
				if (commandLine.hasOption("hostMongo") && commandLine.hasOption("portMongo")) {
					materializeSimpleTable(commandLine);
	
				}
			}
	
			if (commandLine.getArgs() == null) {
				HelpFormatter formater = new HelpFormatter();
				formater.printHelp("Parameter", getOption());
				log.info("Test");
	
			}
	
		} catch (Exception e) {
			log.error("", e);
		}
	
	}

	private static void materializeSimpleTable(CommandLine commandLine) throws Exception {
		if(commandLine.hasOption("target") && commandLine.hasOption("source")){
			if(commandLine.hasOption("fk") && commandLine.hasOption("fk")){
				MongoConnectionProperties mongo = new MongoConnectionProperties();
				mongo.setConnectionProperties(commandLine.getOptionValue("hostMongo"), commandLine.getOptionValue("portMongo"));
				NoSQLLoader nosql = new NoSQLLoader();
				if(commandLine.hasOption("databaseName")){
					nosql.setUpdateableDataContext(mongo.getDB(commandLine.getOptionValue("databaseName")));
					nosql.setSchemaName(commandLine.getOptionValue("databaseName"));
				}else{
					throw new Exception("Missing parameter databaseName");
				}
//				nosql.setUpdateableDataContext(mongo.getDB());
				nosql.materializeSimpleData(commandLine.getOptionValue("target"), commandLine.getOptionValue("source"), 
						commandLine.getOptionValue("fk"), commandLine.getOptionValue("pk"));
				
			}
			
		}

	
	}

	private static void startIntDatabase(String username, String password, String url) throws Exception {
		Database db = new Database();
		db.setConnectionProperties(url, username, password);
		db.initBSBMDatabase();
	}

	private static void startIntDatabase(String username, String password, String url, String path) throws Exception {
		Database db = new Database();
		db.setConnectionProperties(url, username, password);
		db.initBSBMDatabase(path);

	}

	private static void startParseToMongoDB(CommandLine commandLine) throws Exception {
		log.info("Start Parse to Mongodb");
		MySQL mysql = new MySQL();
		mysql.setConnectionProperties(commandLine.getOptionValue("urlMysql"), commandLine.getOptionValue("u"),
				commandLine.getOptionValue("p"));

		MongoConnectionProperties mongo = new MongoConnectionProperties();
		mongo.setConnectionProperties(commandLine.getOptionValue("hostMongo"), commandLine.getOptionValue("portMongo"));
		NoSQLLoader nosql = new NoSQLLoader();
		if(commandLine.hasOption("databaseName")){
			nosql.setUpdateableDataContext(mongo.getDB(commandLine.getOptionValue("databaseName")));
			nosql.setSchemaName(commandLine.getOptionValue("databaseName"));
		}else{
			throw new Exception("Missing parameter databaseName");
		}
		
		if (commandLine.hasOption("d")) {
			nosql.deleteDatabase();
		}

		for (Table table : mysql.getTableMysql("benchmark")) {
			Column[] column = mysql.getColumnMysql(table.getName(), "benchmark");
			nosql.createTable(table, column);
			nosql.insertRows(table, column, mysql.getRowsMysql(table, "benchmark"));
		}

		log.info("Done");
	}

	private static Options getOption(){
		Options options = new Options();
	
		options.addOption("h", "help", false, "Show help");
		options.addOption("importToMysql", false, "Start to import the data to MySQL");
		options.addOption("u", "userMysql", true, "The username in MySQL");
		options.addOption("p", "passwordMysql", true, "The password in MySQL");
		options.addOption("urlMysql", true, "The jdbc-url for MySQL. For example: jdbc:mysql://localhost/benchmark");
		options.addOption("portMongo", true, "The username in MongoDB");
		options.addOption("hostMongo", true, "the password in MongoDB");
		options.addOption("parseToMongo", false, "Start to parse the MySQl databaste to a MongoDB database");
		options.addOption("file", true, "Use your BSBM sqlfiles");
		options.addOption("d", "deleteDatabase", false, "Delete an existing NoSQL Database");
		options.addOption("databaseName", true, "Set the name of the NoSQL database");
		options.addOption("materializeMongo", false, "Materialize a N to One Relationship in MongoDB");
		options.addOption("target", true, "The target table with the forgein key");
		options.addOption("source", true, "The source table with the primary key");
		options.addOption("fk", "forgeinKey", true, "The forgein key");
		options.addOption("pk", "primayKey", true, "The primary key");
		
		return options;
		
	}

	private static boolean hasMySQLConnectionProperties(CommandLine commandLine) throws Exception {
		boolean hasProperties = false;

		if (commandLine.hasOption("u") && commandLine.hasOption("urlMysql")) {
			hasProperties = true;
		} else {
			log.error("The programm need username, password and jdbc-url for the mysqlServer! \n\t"
					+ "For more Inforamtion use -h or --help!");
		}

		return hasProperties;
	}

}
