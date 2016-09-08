package org.aksw.es.bsbmloader.writer;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.aksw.es.bsbmloader.embeddedH2Server.EmbeddedH2Server;
import org.aksw.es.bsbmloader.embeddedmongoserver.EmbeddedMongoServer;
import org.aksw.es.bsbmloader.reader.TableDeleter;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.mongodb.MongoDbDataContext;
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
	}
	
	@After
	public void teardown() throws SQLException{
		new EmbeddedH2Server().dropTestTable();
	}
	
	
	@Test
	public void testTableCreator(){
		creator.setDataContext(createTargetMongo());
		creator.createTable(createSource(), null);	
		assertEquals("TESTTABLE", creator.getDataContext().getTableByQualifiedLabel("TESTTABLE").getName());
	}
	
	@Test
	public void testTableCreatorWithQueryWriter(){
		Table h2 = createSource();
		JdbcDataContext target = testTargetH2();
		creator.setDataContext(target);
		creator.createTable(h2, target.getQueryRewriter() );
		assertEquals(1, creator.getDataContext().getDefaultSchema().getTableCount());
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
