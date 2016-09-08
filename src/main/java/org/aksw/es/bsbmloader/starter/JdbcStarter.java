package org.aksw.es.bsbmloader.starter;

import org.aksw.es.bsbmloader.connectionproperties.JdbcConnectionProperties;
import org.aksw.es.bsbmloader.reader.DataReader;
import org.apache.commons.cli.CommandLine;

public class JdbcStarter implements Starter {

	public void startMaterializeComplex(CommandLine commandLine) throws Exception {
		throw new Exception("Jdbc does not support this");

	}

	public void startMaterializeSimple(CommandLine commandLine) throws Exception {
		throw new Exception("Jdbc does not support this");
	}

	public DataReader createDataReader(CommandLine commandLine) throws Exception {
		JdbcConnectionProperties jdbc = new JdbcConnectionProperties();
		if (interpretCommandline(commandLine)) {
			jdbc.setConnectionProperties(commandLine.getOptionValue("targetUrl"), commandLine.getOptionValue("user"),
					commandLine.getOptionValue("password"));
		}

		DataReader reader = new DataReader();
		reader.setDataContext(jdbc.getDB());

		return reader;
	}

	private boolean interpretCommandline(CommandLine commandLine) {
		return commandLine.hasOption("hostNosql") && commandLine.hasOption("user")
				&& commandLine.hasOption("targetUrl");
	}
/**
	@Deprecated //remove
	public NoSQLParser createConnectionProperties(CommandLine commandLine) throws Exception {
		JdbcConnectionProperties jdbc = new JdbcConnectionProperties();
		if (commandLine.hasOption("hostNosql") && commandLine.hasOption("user")) {
			if (commandLine.hasOption("targetUrl")) {
				jdbc.setConnectionProperties(commandLine.getOptionValue("targetUrl"),
						commandLine.getOptionValue("user"), commandLine.getOptionValue("password"));
			}
		}
		NoSQLParser nosql = new NoSQLParser();
		nosql.setUpdateableDataContext(jdbc.getDB());
		return nosql;
	}**/

}
