package org.aksw.es.bsbmloader.embeddedH2Server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class H2ServerTest {
	private Connection conn = null;
	private EmbeddedH2Server h2Server = new EmbeddedH2Server();
	private String tab = "testTable";

	@Before
	public void setupH2Server() {
		conn = h2Server.getClient();
	}

	@After
	public void closeH2Server() {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		h2Server.shutdown();
	}
	
	@Test
	public void simpleSelectQuery() throws SQLException{
		Statement stmt = conn.createStatement();
		String countQuery = "SELECT * FROM " + tab;
		ResultSet countRS = stmt.executeQuery(countQuery);
		assertNotNull(countRS.next());
		assertEquals(1, countRS.getInt("ID"));
		assertEquals("Hello World!", countRS.getString(2));
	}



}
