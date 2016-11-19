package org.aksw.es.bsbmloader.bsbmloader;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;

public class MainParams {
	
	@Parameter(names = { "-horizontal" }, description = "url of the target database")
	public boolean horizontal = false;

	@Parameter(names = { "-x" })
	public int key;
	
	@Parameter(names = { "-y" })
	public int result;
	
	@Parameter(names = {"-column"})
	public String column;

	@Parameter(names = { "-objectId" }, description = "url of the target database")
	public boolean objectId = false;

	@Parameter(names = { "-secondFkey" }, description = "url of the target database")
	public String secondFkey;

	@Parameter(names = { "-pkSecond" }, description = "url of the target database")
	public String pkSecond;

	@Parameter(names = { "-secondSource" }, description = "url of the target database")
	public String secondSource;

	@Parameter(names = { "-fkJoinTable" }, description = "url of the target database")
	public String fkJoinTable;

	@Parameter(names = { "-target" }, description = "url of the target database")
	public String target;

	@Parameter(names = { "-join" }, description = "url of the target database")
	public String join;

	@Parameter(names = { "-fk" }, description = "url of the target database")
	public String fk;

	@Parameter(names = { "-pk" }, description = "url of the target database")
	public String pk;

	@Parameter(names = { "-source" }, description = "url of the target database")
	public String source;

	@Parameter(names = { "-materializeCouch" }, description = "url of the target database")
	public boolean materializeCouch = false;

	@Parameter(names = { "-materializeMongo" }, description = "url of the target database")
	public boolean materializeMongo = false;

	@Parameter(names = { "-materializeElastic" }, description = "url of the target database")
	public boolean materializeElastic = false;

	@Parameter(names = { "-delete" }, description = "url of the target database")
	public boolean delete = false;

	@Parameter(names = { "-importCouch" }, description = "url of the target database")
	public boolean importCouchdb = false;

	@Parameter(names = { "-couchPassword" }, description = "password")
	public String couchPassword;

	@Parameter(names = { "-importJdbc" }, description = "url of the target database")
	public boolean importJdbc = false;

	@Parameter(names = { "-importElastic" }, description = "url of the target database")
	public boolean importEleastic = false;

	@Parameter(names = { "-importExcel" }, description = "url of the target database")
	public boolean importExcel = false;

	@Parameter(names = { "-couchUser" }, description = "user name")
	public String couchUser;

	@Parameter(names = { "-importMongo" }, description = "url of the target database")
	public boolean importMongodb = false;

	@Parameter(names = { "-targetUrl" }, description = "url of the target database")
	public String targetUrl;

	@Parameter(names = { "-sourceUrl" }, description = "url of the source database")
	public String sourceUrl;

	@Parameter(names = { "-database" }, description = "database")
	public String databaseName;

	@Parameter(names = { "-user" }, description = "user name")
	public String user;

	@Parameter(names = { "-password" }, description = "password")
	public String password;

	@Parameter(names = "-tables", description = "The tables")
	public List<String> tables = new ArrayList<String>();

}
