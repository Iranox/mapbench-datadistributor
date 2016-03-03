package org.aksw.es.bsbmloader.main;

import org.apache.commons.cli.CommandLine;

public interface Starter {
	public void materializeComplexTable(CommandLine commandLine) throws Exception;
	public void materializeSimpleTable(CommandLine commandLine) throws Exception;
}
