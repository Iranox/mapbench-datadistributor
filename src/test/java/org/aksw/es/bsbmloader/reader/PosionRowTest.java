package org.aksw.es.bsbmloader.reader;

import static org.junit.Assert.*;

import org.junit.Test;

public class PosionRowTest {
	
	@Test
	public void testGetPosionRow(){
		assertNotNull(new PosionRow());
		assertNotNull(PosionRow.posionRow);
	}
	
	@Test
	public void testPosionRoe(){
		assertEquals(0, PosionRow.posionRow.size());
//		assertEquals(0, PosionRow.posionRow.indexOf(new Sel);
//		assertEquals(0, PosionRow.posionRow.indexOf((Column) new Object()));
	}

}
