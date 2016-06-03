package org.aksw.es.bsbmloader.connectionproperties;

import java.io.File;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.excel.ExcelDataContext;

public class ExcelPath {
	  
	  
	  public UpdateableDataContext getDB(String path){
		  final File srcFile = new File(path);
		  UpdateableDataContext  dataContext = new ExcelDataContext(srcFile);
		  return dataContext;	 
	  }

}
