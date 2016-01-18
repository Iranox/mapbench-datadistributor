package org.aksw.es.bsbmloader.main;


import java.sql.SQLException;

import org.aksw.es.bsbmloader.loader.Database;
import org.aksw.es.bsbmloader.metamodell.MongoDB;
import org.aksw.es.bsbmloader.metamodell.MySQL;
import org.aksw.es.bsbmloader.metamodell.NoSQLLoader;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.apache.commons.cli.CommandLine;


public class Main 
{
	private static InputReader t = new  InputReader();
	private static MySQL mysql;
	private static org.apache.log4j.Logger log = Logger.getLogger(Main.class);

	
    public static void main( String[] args ){
    	CommandLineParser parser = new BasicParser();
    	Options options = new Options();
    	
    	options.addOption("h", "help", false, "Show help");
    	options.addOption("importToMySQL", false, "Start to import the data to MySQL");
    	options.addOption("u","userMySQL",true,"The username in MySQL");
    	options.addOption("p","passwordMySQL",true,"The password in MySQL");
    	options.addOption("urlMySQL",true,"The jdbc-url for MySQL. For example: jdbc:mysql://localhost/benchmark");
    	options.addOption("portMongo",true,"The username in MongoDB");
    	options.addOption("hostMongo",true,"the password in MongoDB");
    	options.addOption("parseToMongo",false,"Start to parse the MySQl databaste to a MongoDB database");
    	
    	
    	try{
    		CommandLine commandLine = parser.parse(options, args);
    		if(commandLine.hasOption("h")){
    			HelpFormatter formater = new HelpFormatter();
    			formater.printHelp("Parameter", options);
    		}
    		
    		if(commandLine.hasOption("importToMySQL") && hasMySQLConnectionProperties(commandLine)){
    			Database db = new Database();
    			db.setConnectionProperties(commandLine.getOptionValue("urlMySQL"), commandLine.getOptionValue("u"), commandLine.getOptionValue("p"));
    			db.initBSBMDatabase();
    			
    		}
    		
    		if(commandLine.hasOption("parseToMongo") && hasMySQLConnectionProperties(commandLine)){
    			if(commandLine.hasOption("hostMongo") && commandLine.hasOption("portMongo")){
    				MySQL mysql = new MySQL();
    				mysql.setConnectionProperties(commandLine.getOptionValue("urlMySQL"), commandLine.getOptionValue("u"), commandLine.getOptionValue("p"));
    				MongoDB mongo = new MongoDB();
    				mongo.setConnectionProperties(commandLine.getOptionValue("hostMongo"), commandLine.getOptionValue("portMongo"));
    				NoSQLLoader nosql = new NoSQLLoader();
    				nosql.insertSimpleTable(mongo.getDB(), mysql.readDataBase());
    				
    			}		
    			else
    				log.error("The programm need username, password and jdbc-url for the mysqlServer! \n\t"
    						+ "For more Inforamtion use -h or --help!");	
    		}
    		
    	}catch(Exception e){
    	     log.error(e.getMessage());
    	}
    	
    	
   }
    
    private static void startIntDatabase(String username, String password, String url){
    	 Database db = new Database();
         db.setConnectionProperties(url, username, password);
         try{
         	db.initBSBMDatabase();
         }catch(SQLException e){
         	log.error(e.getMessage() + " " + e.getErrorCode());
         }	
    }
    
    private static void startParseToMongoDB(){ 	
        mysql = setMySql();
    	String hostname =  t.readInput("Please insert MongoDB Hostname: ");
        String port = t.readInput("Please insert MongoDB Portnumber: ");
    	log.info("Start Parse to Mongodb");
    	log.info("Done");
    }
    
    private static MySQL setMySql(){
    	mysql = new MySQL();
    	return mysql;
    	
    }
    
    private static boolean hasMySQLConnectionProperties(CommandLine commandLine){
    	boolean hasProperties = false;
    	try{
    		if(commandLine.hasOption("u")  && commandLine.hasOption("urlMySQL"))
    			hasProperties = true;	
			else
				log.error("The programm need username, password and jdbc-url for the mysqlServer! \n\t"
						+ "For more Inforamtion use -h or --help!");	
    		
    	}catch(Exception e){
    		
    	}
    	
    	return hasProperties;
    }
    
    
    

}
    	  