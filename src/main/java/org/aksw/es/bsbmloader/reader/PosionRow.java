package org.aksw.es.bsbmloader.reader;

import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;

public class PosionRow {
	
	
	public Row  getPosionRow(){
		Row  defaultRow = new Row() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public int size() {
				// TODO Auto-generated method stub
				return 0;
			}
			
			public int indexOf(Column column) {
				// TODO Auto-generated method stub
				return 0;
			}
			
			public int indexOf(SelectItem item) {
				// TODO Auto-generated method stub
				return 0;
			}
			
			public Object[] getValues() {
				// TODO Auto-generated method stub
				return null;
			}
			
			public Object getValue(int index) throws IndexOutOfBoundsException {
				// TODO Auto-generated method stub
				return null;
			}
			
			public Object getValue(Column column) {
				// TODO Auto-generated method stub
				return null;
			}
			
			public Object getValue(SelectItem item) {
				// TODO Auto-generated method stub
				return null;
			}
			
			public Row getSubSelection(DataSetHeader header) {
				// TODO Auto-generated method stub
				return null;
			}
			
			public Row getSubSelection(SelectItem[] selectItems) {
				// TODO Auto-generated method stub
				return null;
			}
			
			public Style[] getStyles() {
				// TODO Auto-generated method stub
				return null;
			}
			
			public Style getStyle(int index) throws IndexOutOfBoundsException {
				// TODO Auto-generated method stub
				return null;
			}
			
			public Style getStyle(Column column) {
				// TODO Auto-generated method stub
				return null;
			}
			
			public Style getStyle(SelectItem item) {
				// TODO Auto-generated method stub
				return null;
			}
			
			public SelectItem[] getSelectItems() {
				// TODO Auto-generated method stub
				return null;
			}
		};
		 
		 
		 return defaultRow;
	}

}
