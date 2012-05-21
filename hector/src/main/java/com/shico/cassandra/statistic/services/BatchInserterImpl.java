package com.shico.cassandra.statistic.services;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public class BatchInserterImpl implements BatchInserter {
	private static final Log logger = LogFactory.getLog(BatchInserterImpl.class);
	
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

	@Value("${keyspace}")
	private String keyspaceName;
//	@Autowired
	private String columnFamily;
	@Autowired
	private Cluster cassandraCluster;
	@Autowired
	private Keyspace keyspace;
	
	@Override
	public void doBatchInsert() {
		try{
			getMutator().execute();
		} catch (HectorException he) {
			logger.error("Error in executing batch inserts. ", he);
			throw new RuntimeException("Failed ot execute batch inserts.", he);
		}finally{
			cassandraCluster.getConnectionManager().shutdown();
		}
	}

	@Override
	public void addStringColumn(String name, String value) {
        HColumn<String, String> col = new HColumnImpl<String, String>(StringSerializer.get(), StringSerializer.get());
        col.setName(name);
        col.setValue(value);
        col.setClock(System.currentTimeMillis());
        getMutator().addInsertion(UUID.randomUUID(), columnFamily, col);		
	}

	@Override
	public void addLongColumn(String name, long value){
		HColumn<String, Long> col = new HColumnImpl<String, Long>(StringSerializer.get(), LongSerializer.get());
		col.setName(name);
		col.setValue(value);
		col.setClock(System.currentTimeMillis());
		getMutator().addInsertion(UUID.randomUUID(), columnFamily, col);
	}
	
	@Override
	public void addDateColumn(String name, long value){
		HColumn<String, UUID> col = new HColumnImpl<String, UUID>(StringSerializer.get(), UUIDSerializer.get());
		col.setName(name);
		col.setValue(TimeUUIDUtils.getTimeUUID(value));
		col.setClock(System.currentTimeMillis());
		getMutator().addInsertion(UUID.randomUUID(), columnFamily, col);
	}

	@Override
	public void addDateColumn(String name, Date value){
		addDateColumn(name, value.getTime());
	}

	@Override
	public String columnsAsString(List<HColumn<String, ByteBuffer>> columnList){
		StringBuilder sb = new StringBuilder("{");
    	for (HColumn<String, ByteBuffer> hColumn : columnList) {
    		sb.append("[").append(hColumn.getName()).append("=").append(columnValueAsString(hColumn)).append("]");
		}
    	sb.append("}");
    	return sb.toString();
	}
	
	@Override
	public String columnValueAsString(HColumn<String, ByteBuffer> column){
		String type = getColumnType(column.getName());
		if(type.equals("org.apache.cassandra.db.marshal.UTF8Type")){
			return byteBufferAsString(column.getValue());
		}else if(type.equals("org.apache.cassandra.db.marshal.LongType")){
			return String.valueOf(column.getValue().asLongBuffer().get());
		}else if(type.equals("org.apache.cassandra.db.marshal.TimeUUIDType")){
			UUID uuid = TimeUUIDUtils.uuid(column.getValueBytes());
			long timeFromUUID = TimeUUIDUtils.getTimeFromUUID(uuid);
			return df.format(new Date(timeFromUUID));
		}
		return byteBufferAsString(column.getValue());
	}

	private static Map<String, String> columnDefinitions = new HashMap<String, String>();
	private Map<String, String> getColumnDefinitions(){
		if(columnDefinitions == null){
			columnDefinitions = new HashMap<String, String>();
			KeyspaceDefinition ksdef = cassandraCluster.describeKeyspace(keyspaceName);
			List<ColumnFamilyDefinition> cfDefs = ksdef.getCfDefs();
			for (ColumnFamilyDefinition cfdef : cfDefs) {
				if(cfdef.getName().equals(columnFamily)){
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
		
	private String getColumnType(String name){
		return getColumnDefinitions().get(name);
	}

	private Mutator<UUID> mutator;
	private Mutator<UUID> getMutator(){
		if(mutator == null){
			mutator = HFactory.createMutator(keyspace, UUIDSerializer.get());
		}
		return mutator;
	}
	
	private static String byteBufferAsString(ByteBuffer bb){
		byte[] bytes = new byte[bb.remaining()];
		bb.get(bytes);
		return new String(bytes);
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	
}
