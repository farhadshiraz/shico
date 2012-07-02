package com.shico.cassandra.statistic.services;

import java.sql.Date;
import java.util.List;
import java.util.UUID;

import me.prettyprint.hector.api.beans.HColumn;

public interface BatchRow {
	void addStringColumn(String name, String value);
	void addLongColumn(String name, long value);
	void addDateColumn(String name, long value);
	void addDateColumn(String name, Date value);	
	UUID getKey();
	@SuppressWarnings("rawtypes")
	List<HColumn> getColumns();
}
