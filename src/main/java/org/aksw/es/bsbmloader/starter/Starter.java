package org.aksw.es.bsbmloader.starter;

import org.aksw.es.bsbmloader.parser.NoSQLParser;
import org.apache.commons.cli.CommandLine;

public interface Starter {
	public void startMaterializeComplex(CommandLine commandLine) throws Exception;
	public void startMaterializeSimple(CommandLine commandLine) throws Exception;
	public NoSQLParser createConnectionProperties(CommandLine commandLine) throws Exception;
}
