package org.aksw.es.bsbmloader.reader;

import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;

public class PosionRow {
	
	
	public static final Row  posionRow = new Row() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public int size() {
				return 0;
			}
			
			public int indexOf(Column column) {
				return 0;
			}
			
			public int indexOf(SelectItem item) {
				return 0;
			}
			
			public Object[] getValues() {
				return null;
			}
			
			public Object getValue(int index) throws IndexOutOfBoundsException {
				return null;
			}
			
			public Object getValue(Column column) {
				return null;
			}
			
			public Object getValue(SelectItem item) {
				return null;
			}
			
			public Row getSubSelection(DataSetHeader header) {
				return null;
			}
			
			public Row getSubSelection(SelectItem[] selectItems) {
				return null;
			}
			
			public Style[] getStyles() {
				return null;
			}
			
			public Style getStyle(int index) throws IndexOutOfBoundsException {
				return null;
			}
			
			public Style getStyle(Column column) {
				return null;
			}
			
			public Style getStyle(SelectItem item) {
				return null;
			}
			
			public SelectItem[] getSelectItems() {
				return null;
			}
		
		 
		
	};

}
