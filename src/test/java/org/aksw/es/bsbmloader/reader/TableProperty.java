package org.aksw.es.bsbmloader.reader;

import org.aksw.es.bsbmloader.embeddedH2Server.EmbeddedH2Server;
import org.apache.metamodel.UpdateableDataContext;

public class TableProperty {
	private UpdateableDataContext dc = null;
	private EmbeddedH2Server h2Server = null;

	public void setDc(UpdateableDataContext dc) {
		this.dc = dc;
	}

	public void setH2Server(EmbeddedH2Server h2Server) {
		this.h2Server = h2Server;
	}

	public UpdateableDataContext getDc() {
		return dc;
	}

	public EmbeddedH2Server getH2Server() {
		return h2Server;
	}

}
