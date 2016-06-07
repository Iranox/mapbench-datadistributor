package org.aksw.es.bsbmloader.connectionbuilder;

import org.aksw.es.bsbmloader.connectionproperties.JdbcConnectionProperties;
import org.aksw.es.bsbmloader.connectionproperties.MongoConnectionProperties;

public class ConnectionBuilder {
	
	public ConnectionDatabase createConnectionProperties(String typ){
		if(typ.equals("mysql")){
			return new JdbcConnectionProperties();
		}
		
		if(typ.equals("mongodb")){
			return new MongoConnectionProperties();
		}
		
		return null;
	}

}
