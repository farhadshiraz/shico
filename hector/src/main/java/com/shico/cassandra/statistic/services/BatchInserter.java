package com.shico.cassandra.statistic.services;

import java.nio.ByteBuffer;
import java.util.List;

import me.prettyprint.hector.api.beans.HColumn;

import org.springframework.context.Lifecycle;

public interface BatchInserter extends Lifecycle{
	void doBatchInsert();
	void newRow(BatchRow row);

	String columnValueAsString(HColumn<String, ByteBuffer> column);
	String columnsAsString(List<HColumn<String, ByteBuffer>> columnList);
}
