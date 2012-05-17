package com.shico.cassandra.statistic;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

public class Trial {
    private static StringSerializer sse = StringSerializer.get();
    private static UUIDSerializer uuidse = UUIDSerializer.get();
    private final static String COLUMN_FAMILY = "event";
    private final static String KEYSPACE = "statistic";
    
    private final static String[] deviceModels = {"Html5VideoPlayer", "ViewRightWebPlayer", "iPhone", "Android3.2", "Air7130OP", "iPad"};
    private final static String[] widgets = {"EPG", "RSS", "Weather", "Facebook", "YouTube"};
	private final static String CHANNEL_CHANGE_WEBTV = "CHANNEL_CHANGE_WEBTV";
	private final static String WEBTV_LOGIN = "WEBTV_LOGIN";
	private final static String WIDGET_ACTIVATION = "WIDGET_ACTIVATION";
	private final static String CHANNEL_CHANGE_STB = "CHANNEL_CHANGE_STB";

	private static Map<String, String> columnDefinitions;
	
    private final static int NUMBER_OF_ROWS = 1000;
    private final static int BATCH_SIZE = 100;
    private final static boolean cut = true;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
        Keyspace keyspace = HFactory.createKeyspace(KEYSPACE, getTestCluster());
    	
        try {
        	// Insert event records
            long now = System.currentTimeMillis();
            for (int i=1; i<=(NUMBER_OF_ROWS/BATCH_SIZE); i++) {
            	Mutator<UUID> mutator = HFactory.createMutator(keyspace, UUIDSerializer.get());
            	for (int j = 1; j <= BATCH_SIZE; j++) {
            		UUID rowKey = UUID.randomUUID();
            		addRandomEvents(mutator, rowKey, i*j, now-(i+j*5));
				}
            	mutator.execute();
			}
            
            findByEventType(CHANNEL_CHANGE_STB, keyspace, "event_type", "customer_ref", "channel_ref", "device_ref", "duration", "event_time");
            
        } catch (HectorException e) {
            e.printStackTrace();
        }
        getTestCluster().getConnectionManager().shutdown();
    }

	private static Cluster getTestCluster(){
        return HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");		
	}
	
	private static void findByEventType(String eventType, Keyspace keyspace, String...columnNames){
        IndexedSlicesQuery<UUID, String, String> query = HFactory.createIndexedSlicesQuery(keyspace, uuidse, sse, sse);
        query.setColumnFamily(COLUMN_FAMILY);
        List<String> queryColumns = new ArrayList<String>();
        for (String colName : columnNames) {
			if(getColumnType(colName).equals("org.apache.cassandra.db.marshal.UTF8Type")){
				queryColumns.add(colName);
			}
		}
        query.setColumnNames(queryColumns);
        query.addEqualsExpression("event_type", eventType);
        QueryResult<OrderedRows<UUID, String, String>> result = query.execute();
                    
        OrderedRows<UUID, String, String> rows = result.get();
        List<Row<UUID, String, String>> list = rows.getList();
        StringBuilder sb = new StringBuilder();
        for (Row<UUID, String, String> row : list) {
        	sb.append("key=").append(row.getKey()).append("{");
        	ColumnSlice<String, String> columnSlice = row.getColumnSlice();
        	List<HColumn<String, String>> columns = columnSlice.getColumns();
        	for (HColumn<String, String> hColumn : columns) {
        		sb.append("[").append(hColumn.getName()).append("=").append(hColumn.getValue()).append("]");
			}
        	sb.append("}\n");
		}
        System.out.println("Result: ");
        System.out.println(sb.toString());		
	}
	
	private static String getColumnType(String name){
		return getColumnDefinitions().get(name);
	}
	
	private static Map<String, String> getColumnDefinitions(){
		if(columnDefinitions == null){
			KeyspaceDefinition ksdef = getTestCluster().describeKeyspace(KEYSPACE);
			List<ColumnFamilyDefinition> cfDefs = ksdef.getCfDefs();
			for (ColumnFamilyDefinition cfdef : cfDefs) {
				if(cfdef.getName().equals(COLUMN_FAMILY)){
					List<ColumnDefinition> coldefs = cfdef.getColumnMetadata();
					for (ColumnDefinition coldef : coldefs) {
						String vc = coldef.getValidationClass();
						String name = byteBufferAsString(coldef.getName());
						columnDefinitions.put(name, vc);
					}
				}
			}
		}
		return columnDefinitions;
	}
	
	private static String byteBufferAsString(ByteBuffer bb){
		byte[] bytes = new byte[bb.remaining()];
		bb.get(bytes);
		return new String(bytes);
	}
	
	private static void addChannelChangeEvent(Mutator<UUID> mutator, UUID key, int rowNum, long eventTime) {
		addStringColumn(mutator, key, "event_type", CHANNEL_CHANGE_STB);
		addStringColumn(mutator, key, "customer_ref", "cusotmer_"+rowNum);
		addStringColumn(mutator, key, "channel_ref", "channel_"+rowNum);
		addStringColumn(mutator, key, "device_ref", "device_"+rowNum);
		addLongColumn(mutator, key, "duration", rowNum+100L);
		addDateColumn(mutator, key, "event_time", eventTime);
	}

	private static void addWidgetActivateEvent(Mutator<UUID> mutator, UUID key, int rowNum, long eventTime) {
		addStringColumn(mutator, key, "event_type", WIDGET_ACTIVATION);
		addStringColumn(mutator, key, "customer_ref", "cusotmer_"+rowNum);
		addStringColumn(mutator, key, "device_ref", "device_"+rowNum);
		addStringColumn(mutator, key, "widget", widgets[rowNum % widgets.length]);
		addDateColumn(mutator, key, "event_time", eventTime);
	}

	private static void addWebtvLoginEvent(Mutator<UUID> mutator, UUID key, int rowNum, long eventTime) {
		addStringColumn(mutator, key, "event_type", WEBTV_LOGIN);
		addStringColumn(mutator, key, "customer_ref", "cusotmer_"+rowNum);
		addStringColumn(mutator, key, "device_model", deviceModels[rowNum % deviceModels.length]);
		addDateColumn(mutator, key, "event_time", eventTime);
		addStringColumn(mutator, key, "webtv_session_id", key.toString()+"_"+rowNum);
		addStringColumn(mutator, key, "webtv_username", "webtvuser_"+rowNum);
	}
		
	private static void addWebtvChannelChangeEvent(Mutator<UUID> mutator, UUID key, int rowNum, long eventTime) {
		addStringColumn(mutator, key, "event_type", CHANNEL_CHANGE_WEBTV);
		addStringColumn(mutator, key, "customer_ref", "cusotmer_"+rowNum);
		addStringColumn(mutator, key, "channel_ref", "channel_"+rowNum);
		addStringColumn(mutator, key, "webtv_session_id", key.toString()+"_"+rowNum);
		addStringColumn(mutator, key, "webtv_username", "webtvuser_"+rowNum);
		addStringColumn(mutator, key, "device_model", deviceModels[rowNum % deviceModels.length]);
		addLongColumn(mutator, key, "duration", rowNum+100L);
		addDateColumn(mutator, key, "event_time", eventTime);
	}

	private static void addStringColumn(Mutator<UUID> mutator, UUID key, String name, String value) {
        HColumn<String, String> col = new HColumnImpl<String, String>(sse, sse);
        col.setName(name);
        col.setValue(value);
        mutator.addInsertion(key, COLUMN_FAMILY, col);		
	}

	private static void addLongColumn(Mutator<UUID> mutator, UUID key, String name, long value){
		HColumn<String, Long> col = new HColumnImpl<String, Long>(sse, LongSerializer.get());
		col.setName(name);
		col.setValue(value);
		mutator.addInsertion(key, COLUMN_FAMILY, col);
	}

	private static void addDateColumn(Mutator<UUID> mutator, UUID key, String name, long value){
		HColumn<String, UUID> col = new HColumnImpl<String, UUID>(sse, UUIDSerializer.get());
		col.setName(name);
		col.setValue(TimeUUIDUtils.getTimeUUID(value));
		mutator.addInsertion(key, COLUMN_FAMILY, col);
	}

	private static void addRandomEvents(Mutator<UUID> mutator, UUID key, int rowNum, long eventTime){
		int random = (rowNum / 10) % 4;
		if(random == 0){
			addChannelChangeEvent(mutator, key, rowNum, eventTime);
		}else if(random == 1){
			addWidgetActivateEvent(mutator, key, rowNum, eventTime);
		}else if(random == 2){
			addWebtvChannelChangeEvent(mutator, key, rowNum, eventTime);
		}else if(random == 3){
			addWebtvLoginEvent(mutator, key, rowNum, eventTime);
		}
	}
}
