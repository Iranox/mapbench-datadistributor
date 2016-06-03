package org.aksw.es.bsbmloader.starter;

import org.aksw.es.bsbmloader.connectionproperties.ExcelPath;
import org.aksw.es.bsbmloader.parser.NoSQLParser;
import org.apache.commons.cli.CommandLine;

public class ExcelStarter implements Starter {

	public void startMaterializeComplex(CommandLine commandLine) throws Exception {
		throw new Exception("Not supported");
		
	}

	public void startMaterializeSimple(CommandLine commandLine) throws Exception {
		throw new Exception("Not supported");
		
	}

	public NoSQLParser createConnectionProperties(CommandLine commandLine) throws Exception {
		ExcelPath excel = new ExcelPath();
		NoSQLParser nosql = new NoSQLParser();
		nosql.setUpdateableDataContext(excel.getDB(commandLine.getOptionValue("excelFile")));
		return nosql;
	}

}
