package org.aksw.es.bsbmloader.bsbmloader;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

//TODO Rename
public class Bsbmloader {
	private static org.apache.log4j.Logger log = Logger.getLogger(Bsbmloader.class);

	@Parameter(names = { "-objectId" }, description = "url of the target database")
	private boolean objectId = false;

	@Parameter(names = { "-secondFkey" }, description = "url of the target database")
	private String secondFkey;

	@Parameter(names = { "-pkSecond" }, description = "url of the target database")
	private String pkSecond;

	@Parameter(names = { "-secondSource" }, description = "url of the target database")
	private String secondSource;

	@Parameter(names = { "-fkJoinTable" }, description = "url of the target database")
	private String fkJoinTable;

	@Parameter(names = { "-target" }, description = "url of the target database")
	private String target;

	@Parameter(names = { "-join" }, description = "url of the target database")
	private String join;

	@Parameter(names = { "-fk" }, description = "url of the target database")
	private String fk;

	@Parameter(names = { "-pk" }, description = "url of the target database")
	private String pk;

	@Parameter(names = { "-source" }, description = "url of the target database")
	private String source;

	@Parameter(names = { "-materializeCouch" }, description = "url of the target database")
	private boolean materializeCouch = false;

	@Parameter(names = { "-materializeMongo" }, description = "url of the target database")
	private boolean materializeMongo = false;

	@Parameter(names = { "-materializeElastic" }, description = "url of the target database")
	private boolean materializeElastic = false;

	@Parameter(names = { "-delete" }, description = "url of the target database")
	private boolean delete = false;

	@Parameter(names = { "-importCouch" }, description = "url of the target database")
	private boolean importCouchdb = false;

	@Parameter(names = { "-couchPassword" }, description = "password")
	private String couchPassword;

	@Parameter(names = { "-importJdbc" }, description = "url of the target database")
	private boolean importJdbc = false;

	@Parameter(names = { "-importElastic" }, description = "url of the target database")
	private boolean importEleastic = false;

	@Parameter(names = { "-importExcel" }, description = "url of the target database")
	private boolean importExcel = false;

	@Parameter(names = { "-couchUser" }, description = "user name")
	private String couchUser;

	@Parameter(names = { "-importMongo" }, description = "url of the target database")
	private boolean importMongodb = false;

	@Parameter(names = { "-targetUrl" }, description = "url of the target database")
	private String targetUrl;

	@Parameter(names = { "-sourceUrl" }, description = "url of the source database")
	private String sourceUrl;

	@Parameter(names = { "-database" }, description = "database")
	private String databaseName;

	@Parameter(names = { "-user" }, description = "user name")
	private String user;

	@Parameter(names = { "-password" }, description = "password")
	private String password;

	@Parameter(names = "-tables", description = "The tables")
	private List<String> tables = new ArrayList<String>();

	public static void main(String[] args) throws Exception {

		Bsbmloader main = new Bsbmloader();
		new JCommander(main, args);
		main.interpretCommandLine();

	}

	public void interpretCommandLine() throws Exception {
		if (importMongodb) {
			log.info("Import to MongoDB");
			startImport("mongodb");
			log.info("Done");
		}

		if (materializeMongo && join == null) {
			log.info("Import to MongoDB");
			startMat("mongodb");
			log.info("Done");
		}

		if (materializeMongo && join != null) {
			log.info("Import to MongoDB");
			// startMatComplex("mongodb");
			log.info("Done");
		}

	}

	// TODO Check main pc if class exists. If not create Class NoSQLMatComplex
	/**
	 * private void startMatComplex(String type) throws Exception{
	 * NoSQLMatComplex nosqlMat = new NoSQLMatComplex();
	 * nosqlMat.setDatabaseName(databaseName);
	 * nosqlMat.createDataContext(sourceUrl, user, password, type);
	 * nosqlMat.createDataContextTarget(targetUrl, user, password, type);
	 * nosqlMat.setTarget(join); nosqlMat.setFk(fk); nosqlMat.setJoin(join);
	 * nosqlMat.setSecondFkey(secondFkey);
	 * nosqlMat.setSecondSource(secondSource); nosqlMat.setPrimary(secondFkey);
	 * nosqlMat.setFkJoinTable(fkJoinTable); nosqlMat.setPkSecond(pkSecond);
	 * nosqlMat.importToTarget(target); }
	 ***/

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
