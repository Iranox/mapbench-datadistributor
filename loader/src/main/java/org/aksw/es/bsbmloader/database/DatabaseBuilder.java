package org.aksw.es.bsbmloader.database;


import java.sql.Connection;



import com.mysql.jdbc.Driver;

import org.apache.log4j.Logger;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResourceLoader;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

public class DatabaseBuilder {
	private SimpleDriverDataSource datasource;
	private Connection connection = null;
	private static org.apache.log4j.Logger log = Logger.getLogger(DatabaseBuilder.class);

	public void setConnectionProperties(String jdbcUrl, String username, String password) {
		datasource = new SimpleDriverDataSource();
		datasource.setDriverClass(Driver.class);
		datasource.setUrl(jdbcUrl);
		datasource.setPassword(password);
		datasource.setUsername(username);
	}

	public void initBSBMDatabase() throws Exception {
		log.info("Start Import Data!");
		ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
		populator.addScript(new ClassPathResource("dataset/01ProductFeature.sql"));
		populator.addScript(new ClassPathResource("dataset/02ProductType.sql"));
		populator.addScript(new ClassPathResource("dataset/03Producer.sql"));
		populator.addScript(new ClassPathResource("dataset/04Product.sql"));
		populator.addScript(new ClassPathResource("dataset/05ProductTypeProduct.sql"));
		populator.addScript(new ClassPathResource("dataset/06ProductFeatureProduct.sql"));
		populator.addScript(new ClassPathResource("dataset/07Vendor.sql"));
		populator.addScript(new ClassPathResource("dataset/08Offer.sql"));
		populator.addScript(new ClassPathResource("dataset/09Person.sql"));
		populator.addScript(new ClassPathResource("dataset/10Review.sql"));

		try {
			connection = DataSourceUtils.getConnection(datasource);
			populator.populate(connection);
		} finally {
			if (connection != null) {
				DataSourceUtils.releaseConnection(connection, datasource);
			}
		}
		log.info("Data Import done!");
	}
	
	public void initBSBMDatabase(String path) throws Exception {
		log.info("Start Import Data!");	

		ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
		populator.addScript(new FileSystemResourceLoader().getResource("/" + path + "/" + "01ProductFeature.sql"));
		populator.addScript(new FileSystemResourceLoader().getResource("/" + path + "/" + "02ProductType.sql"));
		populator.addScript(new FileSystemResourceLoader().getResource("/" + path + "/" + "03Producer.sql"));
		populator.addScript(new FileSystemResourceLoader().getResource("/" + path + "/" +  "04Product.sql"));
		populator.addScript(new FileSystemResourceLoader().getResource("/" + path + "/" + "05ProductTypeProduct.sql"));
		populator.addScript(new FileSystemResourceLoader().getResource("/" + path + "/" + "06ProductFeatureProduct.sql"));
		populator.addScript(new FileSystemResourceLoader().getResource("/" + path + "/" + "07Vendor.sql"));
		populator.addScript(new FileSystemResourceLoader().getResource("/" + path + "/" + "08Offer.sql"));
		populator.addScript(new FileSystemResourceLoader().getResource("/" + path + "/" + "09Person.sql"));
		populator.addScript(new FileSystemResourceLoader().getResource("/" + path + "/" + "10Review.sql"));


		try {
			connection = DataSourceUtils.getConnection(datasource);
			populator.populate(connection);
		} finally {
			if (connection != null) {
				DataSourceUtils.releaseConnection(connection, datasource);
			}
		}
		log.info("Data Import done!");
	}

	// For Testing
	public SimpleDriverDataSource getDatasource() {
		return datasource;
	}

	// For Testing
	public Connection getConnection() {
		return connection;
	}

	// for Testing
	public void setConnection(SimpleDriverDataSource datasource) {
		connection = DataSourceUtils.getConnection(datasource);
	}

}
