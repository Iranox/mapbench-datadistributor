package org.bsbmloader.testrunner;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.bsbmloader.loader.TestDatabase;
import org.bsbsmloader.main.TestInputReader;



public class TestRunner {
	public static void main(String[] args) {
	   Result result = new JUnitCore().run(TestDatabase.class);
	   for(Failure failure: result.getFailures()){
		   System.out.println(failure.toString());
	   }
	   
	   result = new JUnitCore().run(TestInputReader.class);
	   for(Failure failure: result.getFailures()){
		   System.out.println(failure.toString());
	  }
	   
	}
	
}
