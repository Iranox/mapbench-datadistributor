package org.aksw.es.bsbmloader.importer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.aksw.es.bsbmloader.reader.ComplexData;
import org.aksw.es.bsbmloader.reader.ComplexDataReader;
import org.aksw.es.bsbmloader.writer.DataComplexUpdater;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.schema.Table;

import com.fasterxml.jackson.databind.deser.SettableAnyProperty;

public class NoSQLMatComplex extends Import {

	private String join;
	private String fk;
	private String secondFkey;
	private String target;
	private String primary;
	private String secondSource;
	private String pkSecond;
	private String fkJoinTable;
	BlockingQueue<ComplexData> queue;

	public void setTarget(String target) {
		this.target = target;

	}

	public void setFk(String fk) {
		this.fk = fk;

	}

	public void setJoin(String join) {
		this.join = join;

	}

	public void setSecondFkey(String secondFkey) {
		this.secondFkey = secondFkey;

	}

	public void setPrimary(String secondFkey) {
		this.primary = secondFkey;

	}

	public void setSecondSource(String secondSource) {
		this.secondSource = secondSource;

	}

	public void setPkSecond(String pkSecond) {
		this.pkSecond = pkSecond;

	}

	public void setFkJoinTable(String fkJoinTable) {
		this.fkJoinTable = fkJoinTable;

	}

	public void importToTarget(String target) throws Exception {
		queue = new ArrayBlockingQueue<ComplexData>(getBORDER());
		ExecutorService executor = Executors.newFixedThreadPool(getThreadsNumber()+1);
		CountDownLatch latch = new CountDownLatch(getThreadsNumber()+1);

		executor.execute(createDataReader(getDatacontextSource().getTableByQualifiedLabel(target), latch));
		for (UpdateableDataContext targetDatabase : getTargetDataContext()) {
			executor.execute(createDataWriter(getTarget(), latch,
					getDatacontextSource().getTableByQualifiedLabel(target), targetDatabase));
		}

		latch.await();
		executor.shutdown();
	}

	private ComplexDataReader createDataReader(Table table, CountDownLatch latch) throws Exception {
		ComplexDataReader dataReader = new ComplexDataReader(getDatacontextSource(), queue, latch);
		dataReader.setDataContext(getDatacontextSource());
		dataReader.setJoinTable(target);
		dataReader.setForgeinKey(fk);
		dataReader.setPkSecond(pkSecond);
		dataReader.setSecondFkey(secondFkey);
		dataReader.setSecondSource(secondSource);
		return dataReader;

	}

	private DataComplexUpdater createDataWriter(Table table, CountDownLatch latch, Table source,
			UpdateableDataContext dc) throws Exception {
		DataComplexUpdater dataWriter = new DataComplexUpdater(latch, table, queue, dc);
		dataWriter.setForgeinKey(fk);
		return dataWriter;
	}

}
