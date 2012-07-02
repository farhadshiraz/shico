package com.shico.cassandra.statistic.services;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.shico.cassandra.statistic.domain.ChannelChangeEvent;
import com.shico.cassandra.statistic.domain.EventType;
import com.shico.cassandra.statistic.domain.WebtvChannelChangeEvent;
import com.shico.cassandra.statistic.domain.WebtvLoginEvent;
import com.shico.cassandra.statistic.domain.WidgetActivationEvent;

@Service
public class StatEventServiceImpl implements StatEventService {
	private final static Logger logger = LoggerFactory.getLogger(StatEventServiceImpl.class);

	@Value("${event.columnfamily}")
	private String columnFamily;
	@Autowired
	private Keyspace keyspace;
	@Autowired
	@Qualifier("eventPersister")
	private BatchInserter eventPersister;

	@Override
	public void executeBatchInserts() {
		eventPersister.doBatchInsert();
	}

	@Override
	public void addChannelChangeEvent(ChannelChangeEvent event) {
		BatchRow row = new BatchRowImpl();
		row.addStringColumn("event_type", event.eventType.name());
		row.addStringColumn("customer_ref", event.customerRef);
		row.addStringColumn("channel_ref", event.channelRef);
		row.addStringColumn("device_ref", event.deviceRef);
		row.addStringColumn("device_model", event.deviceModel);
		row.addDateColumn("event_time", event.eventTime);
		eventPersister.newRow(row);
	}

	@Override
	public void addWidgetActivateEvent(WidgetActivationEvent event) {
		BatchRow row = new BatchRowImpl();
		row.addStringColumn("event_type", event.eventType.name());
		row.addStringColumn("customer_ref", event.customerRef);
		row.addStringColumn("device_ref", event.deviceRef);
		row.addStringColumn("widget", event.widget);
		row.addDateColumn("event_time", event.eventTime);
		eventPersister.newRow(row);
	}

	@Override
	public void addWebtvLoginEvent(WebtvLoginEvent event) {
		BatchRow row = new BatchRowImpl();
		row.addStringColumn("event_type", event.eventType.name());
		row.addStringColumn("customer_ref", event.customerRef);
		row.addStringColumn("device_model", event.deviceModel);
		row.addDateColumn("event_time", event.eventTime);
		row.addStringColumn("webtv_session_id", event.webtvSessionId);
		row.addStringColumn("webtv_username", event.webtvUsername);
		eventPersister.newRow(row);
	}

	@Override
	public void addWebtvChannelChangeEvent(WebtvChannelChangeEvent event) {
		BatchRow row = new BatchRowImpl();
		row.addStringColumn("event_type", event.eventType.name());
		row.addStringColumn("customer_ref", event.channelRef);
		row.addStringColumn("channel_ref", event.channelRef);
		row.addStringColumn("webtv_session_id", event.webtvSessionId);
		row.addStringColumn("webtv_username", event.webtvUsername);
		row.addStringColumn("device_model", event.deviceModel);
		row.addLongColumn("duration", event.duration);
		row.addDateColumn("event_time", event.eventTime);
		eventPersister.newRow(row);
	}

	@Override
	public List<Row<UUID, String, ByteBuffer>> findByEventType(
			EventType eventType, String startKey, int pageSize,
			String... columnNames) {
		IndexedSlicesQuery<UUID, String, ByteBuffer> query = HFactory
				.createIndexedSlicesQuery(keyspace, UUIDSerializer.get(),
						StringSerializer.get(), ByteBufferSerializer.get());
		query.setColumnFamily(columnFamily);
		query.setColumnNames(columnNames);
		query.setRange(startKey, "", false, columnNames.length);
		query.setRowCount(pageSize);
		query.addEqualsExpression("event_type",
				ByteBuffer.wrap(eventType.name().getBytes()));
		QueryResult<OrderedRows<UUID, String, ByteBuffer>> result = query
				.execute();

		logger.info("Query was performed on " + result.getHostUsed()
				+ " and took " + result.getExecutionTimeMicro()
				+ " microseconds");

		return result.get().getList();
	}

	@Override
	public String columnsAsString(List<HColumn<String, ByteBuffer>> columnList) {
		return eventPersister.columnsAsString(columnList);
	}

	@Override
	public boolean isRunning() {
		return false;
	}

	@Override
	public void start() {
	}

	@Override
	public void stop() {
		eventPersister.stop();
	}

}
