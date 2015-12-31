package org.bsbsmloader.main;


import junit.framework.TestCase;
import java.util.Scanner;

import org.bsbmloader.main.InputReader;



/**
 * Unit test for simple App.
 */
public class TestInputReader extends TestCase{

   public void testInput(){
	   InputReader unit = new InputReader();
	   assertEquals("input",  unit.readInput(new Scanner("input ")));
   }
   
}
