package com.shico.cassandra.statistic.services;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import org.springframework.context.Lifecycle;

import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;

import com.shico.cassandra.statistic.domain.ChannelChangeEvent;
import com.shico.cassandra.statistic.domain.EventType;
import com.shico.cassandra.statistic.domain.WebtvChannelChangeEvent;
import com.shico.cassandra.statistic.domain.WebtvLoginEvent;
import com.shico.cassandra.statistic.domain.WidgetActivationEvent;

public interface StatEventService extends Lifecycle {
	void addChannelChangeEvent(ChannelChangeEvent event);
	void addWidgetActivateEvent(WidgetActivationEvent event);
	void addWebtvLoginEvent(WebtvLoginEvent event);
	void addWebtvChannelChangeEvent(WebtvChannelChangeEvent event);
	void executeBatchInserts();
	List<Row<UUID, String, ByteBuffer>> findByEventType(EventType eventType, String startKey, int pageSize, String...columnNames);
	String columnsAsString(List<HColumn<String, ByteBuffer>> columnList);
}
