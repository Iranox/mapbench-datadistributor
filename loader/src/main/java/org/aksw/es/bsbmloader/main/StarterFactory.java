package org.aksw.es.bsbmloader.main;

import org.apache.commons.cli.CommandLine;

public class StarterFactory {
	
//	FactoryPatter sinnvoll ?
	public Starter getStarter(CommandLine commandLine) throws Exception{
		if(commandLine == null){
			return null;
		}
		
		if(commandLine.hasOption("materializeMongo")){
//			return MongoStarter
		}
		
		if(commandLine.hasOption("materializeCouch")){
//			return CouchStarter
		}
		
		if(commandLine.hasOption("materializeElastic")){
//			return ElasticStarter
		}
		
		return null;
	}

}
