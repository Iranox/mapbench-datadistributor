package org.aksw.es.bsbmloader.embeddedH2Server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class EmbeddedH2Server {
	private Connection conn = null;
	private String tab = "testTable";
	
	public EmbeddedH2Server(){
		try {
			createh2Connection();
			dropTestTable();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public Connection getClient() {
		try {
			createTestTable();
			insertTestData();
		} catch (Exception e) {

		}

		return conn;
	}

	private void createh2Connection() throws ClassNotFoundException, SQLException {
		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection("jdbc:h2:mem:test_mem", "", "");
	}
	
	private void createTestTable() throws SQLException{
		Statement stmt = conn.createStatement();
		String createQ = "CREATE TABLE IF NOT EXISTS " + tab
				+ "(ID INT PRIMARY KEY AUTO_INCREMENT(1,1) NOT NULL, NAME VARCHAR(255))";
		stmt.executeUpdate(createQ);
	}
	
	public void createSecondTestTable() throws SQLException{
		Statement stmt = conn.createStatement();
		String createQ = "CREATE TABLE IF NOT EXISTS helloworld" 
				+ "(ID INT PRIMARY KEY AUTO_INCREMENT(1,1) NOT NULL, NAME VARCHAR(255))";
		stmt.executeUpdate(createQ);
	}
	
	public void insertSecondTestData() throws SQLException{
		Statement stmt = conn.createStatement();
		String insertQ = "INSERT INTO helloworld VALUES(1,'Hello World!')";
		stmt.executeUpdate(insertQ);
		conn.commit();
	}
	
	public void dropTestTable() throws SQLException{
		Statement stmt = conn.createStatement();
		String dropQ = "DROP TABLE IF EXISTS " + tab;
		stmt.executeUpdate(dropQ);
	}
	
	public void dropTTable(String tableName) throws SQLException{
		Statement stmt = conn.createStatement();
		String dropQ = "DROP TABLE IF EXISTS " + tableName;
		stmt.executeUpdate(dropQ);
		dropQ = "DROP TABLE IF EXISTS hello world";
		stmt.executeUpdate(dropQ);
	}
	
	
	private void insertTestData() throws SQLException{
		Statement stmt = conn.createStatement();
		String insertQ = "INSERT INTO " + tab + " VALUES(1,'Hello World!')";
		stmt.executeUpdate(insertQ);
		conn.commit();
	}

	public void shutdown() {
		if (conn != null) {
			try {
				closeConnection();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private void closeConnection() throws SQLException {
		conn.close();
	}

}
