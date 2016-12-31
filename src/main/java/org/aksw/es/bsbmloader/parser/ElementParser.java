package org.aksw.es.bsbmloader.parser;

import java.sql.Timestamp;
import java.util.Date;

public class ElementParser {

	public static java.util.Date getDate(Object timeObject) {
		Date time = null;
		if (timeObject instanceof java.sql.Date) {
			time = new Date(((java.sql.Date) timeObject).getTime());
		}

		if (timeObject instanceof java.sql.Timestamp) {
			time = new Date(((Timestamp) timeObject).getTime());
		}

		return time;
	}

}
