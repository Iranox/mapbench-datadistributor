package org.aksw.es.bsbmloader.bsbmloader;


import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;

//TODO Rename
public class Bsbmloader extends MainParams {
	private static org.apache.log4j.Logger log = Logger.getLogger(Bsbmloader.class);

	public static void main(String[] args) throws Exception {

		Bsbmloader main = new Bsbmloader();
		new JCommander(main, args);
		main.interpretCommandLine();

	}

	private void interpretCommandLine() throws Exception {
		if (importMongodb && !horizontal) {
			log.info("Import to MongoDB");
			startImport("mongodb");
			log.info("Done");
		}
		
		if(importMongodb && horizontal){
			log.info("Import to MongoDB");
			startImportHorizontal("mongodb");
			log.info("Done");
		}

		if (materializeMongo && join == null) {
			log.info("Import to MongoDB");
			startMat("mongodb");
			log.info("Done");
		}

		if (materializeMongo && join != null) {
			log.info("Import to MongoDB");
			startMatComplex("mongodb");
			log.info("Done");
		}

	}
	
	private void startMatComplex(String type) throws Exception {
		NoSQLMatComplex nosqlMat = new NoSQLMatComplex();
		nosqlMat.setDatabaseName(databaseName);
		nosqlMat.createDataContext(targetUrl, user, password, type);
		nosqlMat.createDataContextTarget(targetUrl, user, password, type);
		nosqlMat.setTarget(join);
		nosqlMat.setFk(fk);
		nosqlMat.setJoin(join);
		nosqlMat.setSecondFkey(secondFkey);
		nosqlMat.setSecondSource(secondSource);
		nosqlMat.setPrimary(secondFkey);
		nosqlMat.setFkJoinTable(fkJoinTable);
		nosqlMat.setPkSecond(pkSecond);
		nosqlMat.importToTarget(join);
	}

	private void startMat(String type) throws Exception {
		NoSQLMat nosqlMat = new NoSQLMat();
		nosqlMat.setDatabaseName(databaseName);
		nosqlMat.createDataContext(sourceUrl, user, password, type);
		nosqlMat.createDataContextTarget(targetUrl, user, password, type);
		nosqlMat.setTargetTable(target);
		nosqlMat.setForgeinKey(fk);
		nosqlMat.setPrimary(pk);
		nosqlMat.importToTarget(source);
	}
	
	private void startImportHorizontal(String type) throws Exception{
		NoSQLImport importNosql = new NoSQLImport();
		importNosql.setDatabaseName(databaseName);
		importNosql.createDataContext(sourceUrl, user, password);
		importNosql.createDataContextTarget(targetUrl, user, password, type);
		importNosql.setHorzitalData(column, key, result);
		importNosql.startHorizontalImport(tables.toArray(new String[tables.size()]));
	}
	
	private void startImport(String type) throws Exception {
		NoSQLImport importNosql = new NoSQLImport();
		importNosql.setDatabaseName(databaseName);
		importNosql.createDataContext(sourceUrl, user, password);
		importNosql.createDataContextTarget(targetUrl, user, password, type);
		if (tables.size() != 0) {
			importNosql.startImportVertikal(tables.toArray(new String[tables.size()]));
		} else {
			importNosql.startImport();

		} 
	}

}
