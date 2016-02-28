package org.aksw.es.bsbmloader.parser;

import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;

public class PosionRow {
	
	public Row getPosion(){
		  Row posion = new Row() {
				
				/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

				public int size() {
					// TODO Auto-generated method stub
					return -2;
				}
				
				public int indexOf(Column arg0) {
					// TODO Auto-generated method stub
					return 0;
				}
				
				public int indexOf(SelectItem arg0) {
					// TODO Auto-generated method stub
					return 0;
				}
				
				public Object[] getValues() {
					// TODO Auto-generated method stub
					return null;
				}
				
				public Object getValue(int arg0) throws IndexOutOfBoundsException {
					// TODO Auto-generated method stub
					return null;
				}
				
				public Object getValue(Column arg0) {
					// TODO Auto-generated method stub
					return null;
				}
				
				public Object getValue(SelectItem arg0) {
					// TODO Auto-generated method stub
					return null;
				}
				
				public Row getSubSelection(DataSetHeader arg0) {
					// TODO Auto-generated method stub
					return null;
				}
				
				public Row getSubSelection(SelectItem[] arg0) {
					// TODO Auto-generated method stub
					return null;
				}
				
				public Style[] getStyles() {
					// TODO Auto-generated method stub
					return null;
				}
				
				public Style getStyle(int arg0) throws IndexOutOfBoundsException {
					// TODO Auto-generated method stub
					return null;
				}
				
				public Style getStyle(Column arg0) {
					// TODO Auto-generated method stub
					return null;
				}
				
				public Style getStyle(SelectItem arg0) {
					// TODO Auto-generated method stub
					return null;
				}
				
				public SelectItem[] getSelectItems() {
					// TODO Auto-generated method stub
					return null;
				}
			};
			return posion;
	}

}
