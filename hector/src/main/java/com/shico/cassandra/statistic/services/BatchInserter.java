package com.shico.cassandra.statistic.services;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.List;

import me.prettyprint.hector.api.beans.HColumn;

public interface BatchInserter {
	void doBatchInsert();
	void addStringColumn(String name, String value);
	void addLongColumn(String name, long value);
	void addDateColumn(String name, long value);
	void addDateColumn(String name, Date value);
	String columnValueAsString(HColumn<String, ByteBuffer> column);
	String columnsAsString(List<HColumn<String, ByteBuffer>> columnList);
}
