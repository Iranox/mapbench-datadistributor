package org.aksw.es.bsbmloader.main;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.aksw.es.bsbmloader.connectionproperties.CouchConnectionProperties;
import org.aksw.es.bsbmloader.connectionproperties.MongoConnectionProperties;
import org.aksw.es.bsbmloader.loader.Database;
import org.aksw.es.bsbmloader.metamodell.MySQL;
import org.aksw.es.bsbmloader.metamodell.NoSQLLoader;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.apache.metamodel.data.Row;
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
				MongoConnectionProperties mongo = new MongoConnectionProperties();
				mongo.setConnectionProperties(commandLine.getOptionValue("hostNosql"),
						commandLine.getOptionValue("portNosql"));
				if (commandLine.hasOption("hostNosql") && commandLine.hasOption("portNosql")) {
					NoSQLLoader nosql = new NoSQLLoader();
					if (commandLine.hasOption("databaseName")) {
						nosql.setUpdateableDataContext(mongo.getDB(commandLine.getOptionValue("databaseName")));
						nosql.setSchemaName(commandLine.getOptionValue("databaseName"));
					} else {
						throw new Exception("Missing parameter databaseName");
					}
					startParseToNoSQL(commandLine, nosql);

				}

			}

			if (commandLine.hasOption("parseToCouch") && hasMySQLConnectionProperties(commandLine)) {
				CouchConnectionProperties couch = new CouchConnectionProperties();
				couch.setConnectionProperties(commandLine.getOptionValue("hostNosql"),
						commandLine.getOptionValue("portNosql"));
				if (commandLine.hasOption("hostNosql") && commandLine.hasOption("portNosql")) {
					NoSQLLoader nosql = new NoSQLLoader();
					if (commandLine.hasOption("databaseName")) {
						nosql.setUpdateableDataContext(couch.getDB(commandLine.getOptionValue("userCouch"),
								commandLine.getOptionValue("passwordCouch")));
						nosql.setSchemaName(commandLine.getOptionValue("databaseName"));
					} else {
						throw new Exception("Missing parameter databaseName");
					}
					startParseToNoSQL(commandLine, nosql);

				}

			}

			if (commandLine.hasOption("materializeMongo")) {
				if (commandLine.hasOption("hostNosql") && commandLine.hasOption("portNosql")) {
					MongoStarter.materializeSimpleTable(commandLine);

				}
			}
			
			if (commandLine.hasOption("materializeCouch")) {
				if (commandLine.hasOption("hostNosql") && commandLine.hasOption("portNosql")) {
					CouchStarter.materializeSimpleTable(commandLine);

				}
			}

			if (commandLine.hasOption("materializeMongo") && commandLine.hasOption("join")) {
				if (commandLine.hasOption("hostNosql") && commandLine.hasOption("portNosql")) {
					MongoStarter.materializeComplexTable(commandLine);

				}
			}
			
			if (commandLine.hasOption("materializeCouch") && commandLine.hasOption("join")) {
				if (commandLine.hasOption("hostNosql") && commandLine.hasOption("portNosql")) {
					CouchStarter.materializeComplexTable(commandLine);

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

	private static void startParseToNoSQL(CommandLine commandLine, NoSQLLoader nosql) throws Exception {
		log.info("Start Parse to Mongodb");
		BlockingQueue<Row> queue = new ArrayBlockingQueue<Row>(1000);
		MySQL mysql = new MySQL(queue);
		mysql.setConnectionProperties(commandLine.getOptionValue("urlMysql"), commandLine.getOptionValue("u"),
				commandLine.getOptionValue("p"));

		if (commandLine.hasOption("d")) {
			nosql.deleteDatabase();
		}
		for (Table table : mysql.getTableMysql("benchmark")) {
			Column[] column = mysql.getColumnMysql(table.getName(), "benchmark");
			mysql.setTable(table);

			ArrayList<String> fkColumn = mysql.getFkTable(table.getName(), "benchmark");
			nosql.createTable(table, column, fkColumn);
			nosql.setQueue(queue, table, column);
			Thread test1 = new Thread(mysql);
			Thread test2 = new Thread(nosql);
			test1.start();
			test2.start();
//			new Thread(nosql).start();
			while (test1.isAlive() || test2.isAlive()) {
				Thread.sleep(100);
			}
//			Thread.sleep(1000);

		}

		log.info("Done");
	}

	private static Options getOption() {
		Options options = new Options();
		options.addOption("h", "help", false, "Show help");
		options.addOption("importToMysql", false, "Start to import the data to MySQL");
		options.addOption("u", "userMysql", true, "The username in MySQL");
		options.addOption("p", "passwordMysql", true, "The password in MySQL");
		options.addOption("urlMysql", true, "The jdbc-url for MySQL. For example: jdbc:mysql://localhost/benchmark");
		options.addOption("portNosql", true, "The username in MongoDB");
		options.addOption("hostNosql", true, "the password in MongoDB");
		options.addOption("parseToMongo", false, "Start to parse the MySQl databaste to a MongoDB database");
		options.addOption("file", true, "Use your BSBM sqlfiles");
		options.addOption("d", "deleteDatabase", false, "Delete an existing NoSQL Database");
		options.addOption("databaseName", true, "Set the name of the NoSQL database");
		options.addOption("materializeMongo", false, "Materialize a N to One Relationship in MongoDB");
		options.addOption("target", true, "The target table with the forgein key");
		options.addOption("source", true, "The source table with the primary key");
		options.addOption("fk", "forgeinKey", true, "The forgein key");
		options.addOption("pk", "primayKey", true, "The primary key");
		options.addOption("join", true, "The join table with the forgein key");
		options.addOption("parseToCouch", false, "Import the mysql databaste to couchdb");
		options.addOption("fkJoinTable", true, "The forgeinkey of the join table");
		options.addOption("secondSource", true, "The secound source table");
		options.addOption("pkSecond", true, "The primary key of the second table");
		options.addOption("secondFkey", true, "The second key of the join table");
		options.addOption("passwordCouch", true, "The host for CouchDB");
		options.addOption("userCouch", true, "The host for CouchDB");
		options.addOption("materializeCouch", false, "Materialize a N to One Relationship in MongoDB");
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
