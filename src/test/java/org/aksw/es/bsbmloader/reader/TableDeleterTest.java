package org.aksw.es.bsbmloader.reader;

import static org.junit.Assert.assertEquals;

import org.aksw.es.bsbmloader.embeddedH2Server.EmbeddedH2Server;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TableDeleterTest extends TableProperty {

	@Before
	public void setup() {
		setH2Server(new EmbeddedH2Server());
		setDc(new JdbcDataContext(getH2Server().getClient()));
	}

	@After
	public void teardown() {
		getH2Server().shutdown();
	}

	@Test
	public void deleteTable() {
		TableDeleter tableDeleter = new TableDeleter();
		tableDeleter.setDataContext(getDc());
		tableDeleter.deleteDatabase(getDc().getTableByQualifiedLabel("TESTTABLE"));
		assertEquals(0, getDc().getDefaultSchema().getTableCount());

	}
}
