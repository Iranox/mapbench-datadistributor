package org.aksw.es.bsbmloader.connectionproperties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.aksw.es.bsbmloader.embeddedmongoserver.EmbeddedMongoServer;
import org.apache.metamodel.UpdateableDataContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.mongodb.MongoClient;

public class MongoConnectionPropertiesTest {
	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void testConnection() throws Exception {
		EmbeddedMongoServer mongoServer = new EmbeddedMongoServer();
		MongoClient client = mongoServer.getClient();

		String[] properties = client.getAddress().toString().split(":");

		MongoConnectionProperties connectionProperties = new MongoConnectionProperties();
		connectionProperties.setConnectionProperties(properties[0], properties[1]);
		connectionProperties.setDatabaseName("test");

		UpdateableDataContext dataContext = connectionProperties.getDB();

		assertNotNull(dataContext);
		assertEquals("test", dataContext.getDefaultSchema().getName());
		client.close();
		mongoServer.shutdown();
	}

	@Test
	public void testExepction() throws Exception {
		MongoConnectionProperties connectionProperties = new MongoConnectionProperties();
		exception.expect(Exception.class);
		connectionProperties.setConnectionProperties(null, null, null);
	}

}
