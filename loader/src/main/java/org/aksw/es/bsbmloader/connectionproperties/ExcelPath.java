package org.aksw.es.bsbmloader.connectionproperties;

import java.io.File;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.excel.ExcelDataContext;

public class ExcelPath {
	private UpdateableDataContext dc;
	  
	  
	  public UpdateableDataContext getDB(String path){
		  final File srcFile = new File(path);
		  dc = new ExcelDataContext(srcFile);
		  return dc;	 
	  }

}
