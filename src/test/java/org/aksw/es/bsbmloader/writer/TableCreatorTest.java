package org.aksw.es.bsbmloader.writer;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.aksw.es.bsbmloader.embeddedH2Server.EmbeddedH2Server;
import org.aksw.es.bsbmloader.embeddedmongoserver.EmbeddedMongoServer;
import org.aksw.es.bsbmloader.reader.TableDeleter;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.mongodb.MongoDbDataContext;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.DB;

public class TableCreatorTest {
	
	private TableCreator creator = null;
	
	
	@Before
	public void setup(){
		creator = new TableCreator();
		deleteMongoDB();
	}
	
	@After
	public void teardown() throws SQLException{
		new EmbeddedH2Server().dropTestTable();
		deleteMongoDB();
	}
	
	
	@Test
	public void testTableCreatorWithTwoTargets(){
		Table h2Table = createSource();
		UpdateableDataContext[] dataSourceArray = {testTargetH2(), createTargetMongo()};
		creator.createTableWithMoreTargets(h2Table, dataSourceArray);
		creator.setDataContext(dataSourceArray[0]);
		assertEquals("TESTTABLE", getTableName());
		creator.setDataContext(dataSourceArray[1]);
		assertEquals("TESTTABLE", getTableName());
		
	}
	
	@Test
	public void testTableCreatorVertical(){
		creator.setDataContext(createTargetMongo());
		String[] columnsName = {"Name"};
		creator.createTableVertical(createSource(), columnsName, "id", null);
		assertEquals("TESTTABLE", getTableName());
		deleteMongoDB();
	}
	
	@Test
	public void testTableCreator(){
		creator.setDataContext(createTargetMongo());
		creator.createTable(createSource(), null);
		assertEquals("TESTTABLE",getTableName());
		deleteMongoDB();
	}
	
	private String getTableName(){
		Schema schema = creator.getDataContext().getDefaultSchema();
		Table table = schema.getTableByName("TESTTABLE");
		return table.getName();
	}
	
	@Test
	public void testTableCreatorWithQueryWriter(){
		Table h2Table = createSource();
		JdbcDataContext target = testTargetH2();
		creator.setDataContext(target);
		creator.createTable(h2Table, target.getQueryRewriter() );
		assertEquals("TESTTABLE",getTableName());
	}

	private void deleteMongoDB(){
		 new EmbeddedMongoServer().getClient().getDatabase("test").getCollection("TESTTABLE").drop();
	}
	

	private JdbcDataContext testTargetH2(){
		JdbcDataContext source = new JdbcDataContext(new EmbeddedH2Server().getClient());
		TableDeleter deleter = new TableDeleter();
		deleter.setDataContext(source);
		deleter.deleteDatabase(source.getTableByQualifiedLabel("TESTTABLE"));
		return source;
	}
	
	private Table createSource(){
		UpdateableDataContext source = new JdbcDataContext(new EmbeddedH2Server().getClient());
		return source.getTableByQualifiedLabel("TESTTABLE");
	}
	
	private UpdateableDataContext createTargetMongo(){
		DB databaseMongo = new DB(new EmbeddedMongoServer().getClient(), "test");
		return new MongoDbDataContext(databaseMongo);
	}

}
