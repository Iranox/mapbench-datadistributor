package org.aksw.es.bsbmloader.writer;

import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.jdbc.dialects.IQueryRewriter;
import org.apache.metamodel.schema.Table;

public class TableCreator {
	
	private UpdateableDataContext dataContext;
	
	public UpdateableDataContext getDataContext() {
		return dataContext;
	}

	public void setDataContext(UpdateableDataContext dataContext) {
		this.dataContext = dataContext;
	}

	public void createTable(Table table, final IQueryRewriter typ) {
		UpdateScript script = null;
		script = UpdateScriptWriterHelp.createInsertScript(dataContext, table, typ);
		dataContext.executeUpdate(script);

	}

	public void createTableWithMoreTargets(Table createSource, UpdateableDataContext[] dataSourceArray) {
		for (UpdateableDataContext dataContext : dataSourceArray) {
			this.dataContext = dataContext;
			createTable(createSource, null);
		}
	
	}
}
