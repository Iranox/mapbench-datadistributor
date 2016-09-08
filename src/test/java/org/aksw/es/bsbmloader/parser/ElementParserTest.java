package org.aksw.es.bsbmloader.parser;

import java.sql.Timestamp;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ElementParserTest {
	
	private Date utilDate = null;
	
	@Before
	public void setup() {
		utilDate = new Date();
	}
	
	@Test
	public void testParseTimeSqlDate(){
		java.sql.Date sqlDate = new java.sql.Date( utilDate.getTime());
		Object timeObject = sqlDate;
		assertEquals(utilDate, new ElementParser().getDate(timeObject));
	}
	
	@Test
	public void testParseTimeTimeStamp(){
		Timestamp time = new Timestamp(utilDate.getTime());
		Object timeStampObject = time;
		assertEquals(utilDate, new ElementParser().getDate(timeStampObject));
	}
	
	@Test
	public void testParseInteger(){
		Object numberObject = 42;
		assertEquals(42, new ElementParser().getInteger(numberObject));
	}

}
