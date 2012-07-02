package com.shico.cassandra.statistic.services;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.beans.HColumn;

public class BatchRowImpl implements BatchRow {
	@SuppressWarnings("rawtypes")
	List<HColumn> columns = new ArrayList<HColumn>();
	UUID key;
	
	public BatchRowImpl() {
		super();
		key = UUID.randomUUID();
	}

	@Override
	public void addStringColumn(String name, String value) {
        HColumn<String, String> col = new HColumnImpl<String, String>(StringSerializer.get(), StringSerializer.get());
        col.setName(name);
        col.setValue(value);
        col.setClock(System.currentTimeMillis());
        columns.add(col);
	}

	@Override
	public void addLongColumn(String name, long value){
		HColumn<String, Long> col = new HColumnImpl<String, Long>(StringSerializer.get(), LongSerializer.get());
		col.setName(name);
		col.setValue(value);
		col.setClock(System.currentTimeMillis());
        columns.add(col);
	}
	
	@Override
	public void addDateColumn(String name, long value){
		HColumn<String, UUID> col = new HColumnImpl<String, UUID>(StringSerializer.get(), UUIDSerializer.get());
		col.setName(name);
		col.setValue(TimeUUIDUtils.getTimeUUID(value));
		col.setClock(System.currentTimeMillis());
        columns.add(col);
	}

	@Override
	public void addDateColumn(String name, Date value){
		addDateColumn(name, value.getTime());
	}

	@Override
	public UUID getKey() {
		return key;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List<HColumn> getColumns() {
		return columns;
	}
}
