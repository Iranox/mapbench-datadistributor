package org.aksw.es.bsbmloader.parser;

import java.sql.Timestamp;
import java.util.Date;

public class ElementParser {
	
	public java.util.Date getDate(Object obj) {
		Date time = null;
		if (obj instanceof java.sql.Date) {
			time = new Date(((java.sql.Date) obj).getTime());
		}

		if (obj instanceof java.sql.Timestamp) {
			time = new Date(((Timestamp) obj).getTime());
		}

		return time;
	}

	public int getInteger(Object obj) {
		int i = java.lang.Integer.parseInt(obj.toString());
		return i;
	}


}
