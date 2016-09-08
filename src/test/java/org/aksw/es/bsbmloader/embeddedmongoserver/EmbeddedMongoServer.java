package org.aksw.es.bsbmloader.embeddedmongoserver;

import java.net.InetSocketAddress;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;

public class EmbeddedMongoServer {

	private MongoClient client;
	private MongoServer server;

	public MongoClient getClient() {
		server = new MongoServer(new MemoryBackend());
		InetSocketAddress serverAddress = server.bind();
		client = new MongoClient(new ServerAddress(serverAddress));
		return client;
	}

	public void shutdown() {
		if (client != null) {
			client.close();
		}

		if (server != null) {
			server.shutdown();
		}
	}

}
