package org.aksw.es.bsbmloader.main;



import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;


public class Main {
	private static org.apache.log4j.Logger log = Logger.getLogger(Main.class);
	
	/**

	options.addOption("target", true, "The target table with the forgein key");
	options.addOption("source", true, "The source table with the primary key");
	options.addOption("fk", "forgeinKey", true, "The forgein key");
	options.addOption("pk", "primayKey", true, "The primary key");
	options.addOption("join", true, "The join table with the forgein key");
	options.addOption("fkJoinTable", true, "The forgeinkey of the join table");
	options.addOption("secondSource", true, "The secound source table");
	options.addOption("pkSecond", true, "The primary key of the second table");
	options.addOption("secondFkey", true, "The second key of the join table");
    options.addOption("materializeMongo", false, "Materialize a N to One Relationship in MongoDB");
	options.addOption("materializeCouch", false, "Materialize a N to One Relationship in MongoDB");
	options.addOption("materializeElastic", false, "Materialize a N to One Relationship in Mon
	options.addOption("objectId", false, "use only ObjectId"); **/
	
	@Parameter(names= {"-delete"}, description = "url of the target database")
	private boolean delete = false;
	
	@Parameter(names= {"-importCouch"}, description = "url of the target database")
	private boolean importCouchdb = false;
	
	@Parameter(names= {"-couchPassword"}, description = "password")
	private String couchPassword;
	
	@Parameter(names= {"-importJdbc"}, description = "url of the target database")
	private boolean importJdbc = false;
	
	@Parameter(names= {"-importElastic"}, description = "url of the target database")
	private boolean importEleastic = false;
	
	@Parameter(names= {"-importExcel"}, description = "url of the target database")
	private boolean importExcel = false;
	
	@Parameter(names= {"-couchUser"}, description = "user name")
	private String couchUser;
	
	@Parameter(names= {"-importMongo"}, description = "url of the target database")
	private boolean importMongodb = false;
	
	@Parameter(names= {"-targetUrl"}, description = "url of the target database")
	private String targetUrl;

	@Parameter(names= {"-sourceUrl"}, description = "url of the source database")
	private String sourceUrl;
	
	@Parameter(names= {"-database"}, description = "database")
	private String databaseName;
	
	@Parameter(names= {"-user"}, description = "user name")
	private String user;
	
	@Parameter(names= {"-password"}, description = "password")
	private String password;
	
	
	
	public static void main(String[] args) throws Exception {
		
		Main main = new Main();
		new JCommander(main, args);
		main.interpretCommandLine();
		
	}
	
	public void  interpretCommandLine() throws Exception{
		if(importMongodb){
			log.info("Import to MongoDB");
			startImport("mongodb");
		}
		
	}
	
	private void startImport(String type) throws Exception{
		NoSQLImport importNosql = new NoSQLImport();
		importNosql.setDatabaseName(databaseName);
		importNosql.createDataContext(sourceUrl, user, password);;
		importNosql.createDataContextTarget(targetUrl, user, password,type);;
		importNosql.startImport();
	}
	
	//TODO 	restructuring function 
/**	private static void interpretCommandLine(CommandLine commandLine) throws Exception{
		Starter starter = new StarterFactory().getStarter(commandLine);

		if (commandLine.hasOption("parseToMongo") && hasMySQLConnectionProperties(commandLine)) {
			startParseToNoSQL(commandLine);
		}

		if (commandLine.hasOption("parseToCouch") && hasMySQLConnectionProperties(commandLine)) {
			startParseToNoSQL(commandLine);
		}

		if (commandLine.hasOption("parseToExcel") && hasMySQLConnectionProperties(commandLine)) {
			startParseToNoSQL(commandLine);
		}

		if (commandLine.hasOption("parseToElastic") && hasMySQLConnectionProperties(commandLine)) {
			startParseToNoSQL(commandLine);
		}
		
		if (commandLine.hasOption("parseToJdbc") && hasMySQLConnectionProperties(commandLine)) {
			startParseToNoSQL(commandLine);
		}

		if (commandLine.hasOption("materializeMongo")) {
			if (commandLine.hasOption("hostNosql") && commandLine.hasOption("portNosql")) {
				log.info("start materializeMongo"); //TODO dont trigger by simple mat.
				starter.startMaterializeSimple(commandLine);
				log.info("Done");

			}
		}
		
		if (commandLine.hasOption("materializeElastic")) {
			if (commandLine.hasOption("portNosql")) {
				log.info("start materializeMongo");
				starter.startMaterializeSimple(commandLine);
				log.info("Done");

			}
		}

		if (commandLine.hasOption("materializeCouch")) {
			if (commandLine.hasOption("hostNosql") && commandLine.hasOption("portNosql")) {
				log.info("start materializeCouch");
				starter.startMaterializeSimple(commandLine);
				log.info("Done");

			}
		}

		if (commandLine.hasOption("materializeMongo") && commandLine.hasOption("join")) {
			if (commandLine.hasOption("hostNosql") && commandLine.hasOption("portNosql")) {
				log.info("start materializeMongo");
				starter.startMaterializeComplex(commandLine);
				log.info("Done");

			}
		}

		if (commandLine.hasOption("materializeCouch") && commandLine.hasOption("join")) {
			if (commandLine.hasOption("hostNosql") && commandLine.hasOption("portNosql")) {
				log.info("start materializeCouch");
				starter.startMaterializeComplex(commandLine);
				log.info("Done");

			}
		}
	} **/


}
