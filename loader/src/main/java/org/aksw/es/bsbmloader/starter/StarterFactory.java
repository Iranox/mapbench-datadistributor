package org.aksw.es.bsbmloader.starter;


import org.apache.commons.cli.CommandLine;

public class StarterFactory {
	

	public Starter getStarter(CommandLine commandLine) throws Exception{
		if(commandLine == null){
			return null;
		}
		
		if(commandLine.hasOption("materializeMongo") || commandLine.hasOption("parseToMongo")){
			return new MongoStarter();
		}
		
		if(commandLine.hasOption("materializeCouch") || commandLine.hasOption("parseToCouch")){
            return new CouchStarter();
		}
		
		if(commandLine.hasOption("parseToExcel")){
			return new ExcelStarter();
		}
		
		if(commandLine.hasOption("parseToJdbc")){
			return new JdbcStarter();
		}
		
		if(commandLine.hasOption("materializeElastic") || commandLine.hasOption("parseToElastic")){
			return new ElasticSearchStarter();
		}
		
		return null;
	}

}
