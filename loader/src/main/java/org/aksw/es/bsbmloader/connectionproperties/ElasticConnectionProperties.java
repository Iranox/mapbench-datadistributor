package org.aksw.es.bsbmloader.connectionproperties;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.elasticsearch.common.ElasticSearchMetaData;
import org.apache.metamodel.elasticsearch.nativeclient.ElasticSearchDataContext;
import org.apache.metamodel.elasticsearch.nativeclient.ElasticSearchMetaDataParser;
import org.apache.metamodel.schema.ColumnType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.Map;

public class ElasticConnectionProperties extends ConnectionProperties {

	public void setConnectionProperties(String hostname) {
		setHostname(hostname);
	}

	
	public UpdateableDataContext getDB(String indexName) throws Exception {
		TransportClient client = new TransportClient();
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(getHostname()), 9300));
		UpdateableDataContext dataContext = new ElasticSearchDataContext((Client) client, indexName);
		return dataContext;

	}
	
	public void test(){
		   Map<String, Object> metadata = new LinkedHashMap<String, Object>(); 
	        metadata.put("message", MapBuilder.newMapBuilder().put("type", ColumnType.JAVA_OBJECT).immutableMap()); 
	        ElasticSearchMetaData metaData = ElasticSearchMetaDataParser.parse(metadata); 
	        String[] columnNames = metaData.getColumnNames(); 
	        ColumnType[] columnTypes = metaData.getColumnTypes(); 
	        System.out.println(columnNames[1] + " " + columnTypes[1]);
	}

}
