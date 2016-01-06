package org.bsbmloader.main;


import java.sql.SQLException;
import org.bsbmloader.loader.Database;
import org.bsbmloader.metamodell.MongoDBLoader;
import org.bsbmloader.metamodell.MySQL;



public class Main 
{
	private static InputReader t = new  InputReader();
	private static MySQL mysql;

	
    public static void main( String[] args ){
    	String input ;
    	boolean endlessLoop = true;
    	while(endlessLoop){
    		showOptions();
        	InputReader inputReader = new InputReader();
        	input = inputReader.readInput("Your option: ");
        	try{
        		int i = Integer.parseInt(input);
        		switch(i){
        		   case 1 : 
        			  startIntDatabase();
        		      break;
        		   case 2: 
        			   startParseToMongoDB();
        			   break;
        		   case 3:
        			  endlessLoop = false;
        			  break;
        		    default:
        			   System.out.println("Unknown Service");
        		 }
        	  }catch(Exception e){
        			e.printStackTrace();
        		
        		}
    		
    	}
    	
    	}
    
    private static void startIntDatabase(){
    	 Database db = new Database();
         String username =  t.readInput("Please insert Username: ");
         String password = t.readInput("Please insert Password: ");
         String url = t.readInput("Please insert JDBC-url: ");
         db.setConnectionProperties(url, username, password);
         try{
         	db.initBSBMDatabase();
         }catch(SQLException e){
         	System.out.println(e.getMessage() + " " + e.getErrorCode());
         }	
    }
    
    private static void startParseToMongoDB(){ 	
        mysql = setMySql();
    	String hostname =  t.readInput("Please insert MongoDB Hostname: ");
        String port = t.readInput("Please insert MongoDB Portnumber: ");
        MongoDBLoader mongo = new MongoDBLoader();
    	mongo.setConnectionProperties(hostname,port);
    	System.out.println("Start Parse to Mongodb");
    	mongo.insertVendor(mysql.getAllVendor());
    	mongo.insertProductFeature(mysql.getAllProductFeature());
    	mongo.insertProductType(mysql.getAllProductType());
    	mongo.insertPersons(mysql.getAllPersons());
    	mongo.insertOffer(mysql.getOffer());
        mongo.insertProduct(mysql.getAllProduct());
    	System.out.println("Done");
    }
    
    private static MySQL setMySql(){
    	mysql = new MySQL();
    	String username =  t.readInput("Please insert Username: ");
        String password = t.readInput("Please insert Password (uncrypt): ");
        String url = t.readInput("Please insert JDBC-url: ");
    	mysql.setConnectionProperties(url, username, password);
    	return mysql;
    	
    }
    
    private static void showOptions(){
    	System.out.println("Select an option");
    	System.out.println("Initialize a MySQL database with BSBM. Press 1");
    	System.out.println("Parse from MySQl Benchmark to MongoDB (not Completed). Press 2");
    	System.out.println("Exit. Press 3");
    }
}
    	  