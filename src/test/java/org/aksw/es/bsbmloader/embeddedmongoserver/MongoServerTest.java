package org.aksw.es.bsbmloader.embeddedmongoserver;

import static org.junit.Assert.assertEquals;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.client.MongoCollection;

public class MongoServerTest {

	private MongoCollection<Document> collection;
	private EmbeddedMongoServer mongoserver;

	@Before
	public void setupMongoServer() {
		mongoserver = new EmbeddedMongoServer();
		collection = mongoserver.getClient().getDatabase("testdb").getCollection("testcollection");
	}

	@After
	public void shutdownMongoServer() {
		mongoserver.shutdown();
	}

	@Test
	public void simpleCountQery() {
		assertEquals(0, collection.count());
	}

	@Test
	public void testSimpleInsertQuery() throws Exception {
		Document obj = new Document("_id", 1).append("key", "value");
		collection.insertOne(obj);
		assertEquals(obj, collection.find().first());
	}

}
