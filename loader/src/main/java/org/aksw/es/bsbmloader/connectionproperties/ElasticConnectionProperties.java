package org.aksw.es.bsbmloader.connectionproperties;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.elasticsearch.nativeclient.ElasticSearchDataContext;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;

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

}
