package com.shico.cassandra.statistic;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.shico.cassandra.profiling.Profiler;
import com.shico.cassandra.profiling.ProfilerAspect;
import com.shico.cassandra.statistic.domain.ChannelChangeEvent;
import com.shico.cassandra.statistic.domain.EventType;
import com.shico.cassandra.statistic.domain.WebtvChannelChangeEvent;
import com.shico.cassandra.statistic.domain.WebtvLoginEvent;
import com.shico.cassandra.statistic.domain.WidgetActivationEvent;
import com.shico.cassandra.statistic.services.StatEventService;

public class StatisticDemoWithCassandra {
	private final static Logger logger = LoggerFactory.getLogger(StatisticDemoWithCassandra.class);
	
    private final static String[] deviceModels = {"Html5VideoPlayer", "ViewRightWebPlayer", "iPhone", "Android3.2", "Air7130OP", "iPad"};
    private final static String[] widgets = {"EPG", "RSS", "Weather", "Facebook", "YouTube"};
	    
	@Value("${event.batch.size}")
	private int batchSize;
	@Value("${number.of.rows}")
	private int numberOfRows;
    @Autowired
    private StatEventService statEventService;
    
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ClassPathXmlApplicationContext context = null;
		try {
			context = loadContext();
		} catch (Exception e) {
			System.out.println("Unable ot load application context " + e.getMessage());
			System.exit(0);
		}

		StatisticDemoWithCassandra main = new StatisticDemoWithCassandra();
		AutowireCapableBeanFactory acbf = context.getAutowireCapableBeanFactory();
		acbf.autowireBeanProperties(main, AutowireCapableBeanFactory.AUTOWIRE_BY_TYPE, false);
		acbf.initializeBean(main, "statisticDemoWithCassandra");

		try {
			main.addRandomEvents();
		} catch (Exception e) {
			main.cleanup();
			e.printStackTrace();
			System.exit(0);
		}
		
		
//		main.findByEventType(EventType.WEBTV_LOGIN);
		
		printProfiling();
    	main.cleanup();
    }

	private static void printProfiling(){
		logger.debug(" =============== TIME =============== ");
		ConcurrentMap<String, Pair<Long, Long>> timers = ProfilerAspect.getTimers();
		for (Entry<String, Pair<Long, Long>> entry : timers.entrySet()){
			logger.debug(entry.getKey()+" was invoked "+entry.getValue().getValue1()+ " times and took "+entry.getValue().getValue0() + " milliseconds.");
		}
		logger.debug(" =============== TIME =============== ");
	}
	
	private static ClassPathXmlApplicationContext loadContext()
			throws Exception {
		logger.info("Loading context ....");
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
				new String[] { "classpath:META-INF/spring/applicationContext.xml" });

		return context;
	}

	private void addRandomEvents(){
		// Insert event records
		int recNum = 0;
		long now = System.currentTimeMillis();
		for (int i=1; i<=(numberOfRows/batchSize); i++) {
			for (int j = 1; j <= batchSize; j++) {
				addRandomEvents(i*j, now-(i+j*5));
				recNum++;
			}
			statEventService.executeBatchInserts();
			logger.debug("Number of rows written "+recNum);
		}
	}
 
	private void cleanup(){
		statEventService.stop();
	}
	
	private void findByEventType(EventType eventType){                    
		String startKey = "";
		int pageSize = 500;
		while(true){
	        List<Row<UUID, String, ByteBuffer>> list = statEventService.findByEventType(eventType, startKey, pageSize+1, "event_type", "customer_ref", "channel_ref", "device_ref", "duration", "event_time");
	        StringBuilder sb = new StringBuilder();
	        for (int i = 0; i < Math.min(list.size(), pageSize); i++) {
	        	Row<UUID, String, ByteBuffer> row = list.get(i);
	        	sb.append("key=").append(row.getKey()).append("{");
	        	ColumnSlice<String, ByteBuffer> columnSlice = row.getColumnSlice();
	        	List<HColumn<String, ByteBuffer>> columns = columnSlice.getColumns();
	        	sb.append(statEventService.columnsAsString(columns));
	        	sb.append("}\n");
	        }
	        logger.debug("Result: ");
	        logger.debug(sb.toString());
	        if(list.size() > pageSize){
	        	startKey = list.get(pageSize).getKey().toString();
	        }else{
	        	break;
	        }
		}
	}
	
	@Profiler("RecordMaker")
	private void addRandomEvents(int rowNum, long eventTime){
		int random = (rowNum / 10) % 4;
		String customerRef = "cusotmer_"+rowNum;

		if(random == 1){
			statEventService.addWidgetActivateEvent(new WidgetActivationEvent(customerRef, eventTime, "device_"+rowNum, widgets[rowNum % widgets.length]));
		}else if(random == 2){
			statEventService.addWebtvChannelChangeEvent(new WebtvChannelChangeEvent(customerRef, eventTime, "channel_"+rowNum, deviceModels[rowNum % deviceModels.length], rowNum+100L, UUID.randomUUID().toString(), "webtvuser_"+rowNum));
		}else if(random == 3){
			statEventService.addWebtvLoginEvent(new WebtvLoginEvent(customerRef, eventTime, deviceModels[rowNum % deviceModels.length], UUID.randomUUID().toString()+"_"+rowNum, "webtvuser_"+rowNum));
		}else{
			statEventService.addChannelChangeEvent(new ChannelChangeEvent(customerRef, eventTime, "channel_"+rowNum, "device_"+rowNum, "STB", rowNum+100L));
		}
	}
}
