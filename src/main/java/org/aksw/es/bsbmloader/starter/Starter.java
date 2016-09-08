package org.aksw.es.bsbmloader.starter;

import org.aksw.es.bsbmloader.reader.DataReader;
import org.apache.commons.cli.CommandLine;

public interface Starter {
	public void startMaterializeComplex(CommandLine commandLine) throws Exception;
	public void startMaterializeSimple(CommandLine commandLine) throws Exception;
//	public NoSQLParser createConnectionProperties(CommandLine commandLine) throws Exception;
	public DataReader createDataReader(CommandLine commandLine) throws Exception ;
}
