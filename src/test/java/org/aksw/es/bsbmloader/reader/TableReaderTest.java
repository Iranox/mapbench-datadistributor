package org.aksw.es.bsbmloader.reader;

import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.aksw.es.bsbmloader.embeddedH2Server.EmbeddedH2Server;

public class TableReaderTest extends TableProperty{
	TableReader tableReader = null;

	@Before
	public void setup() {
		setH2Server(new EmbeddedH2Server());
		setDc(new JdbcDataContext(getH2Server().getClient()));
		tableReader = new TableReader(getDc());
	}

	@After
	public void teardown() {
		getH2Server().shutdown();
	}

	@Test
	public void testGetTable() throws Exception {
		Table[] tables = tableReader.getTables("test_men");
		assertEquals("TESTTABLE", tables[0].getName());
	}

	@Test
	public void testGetColumnn() {
		Column col = tableReader.getColumn("test_men", "ID", "TESTTABLE");
		assertEquals("ID", col.getName());

	}

}
