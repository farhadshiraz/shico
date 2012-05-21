package com.shico.cassandra.statistic.domain;

public class ChannelChangeEvent extends StatEvent {
	public String channelRef;
	public String deviceRef;
	public String deviceModel;
	public long duration;
	
	public ChannelChangeEvent(String customerRef,
			long eventTime, String channelRef, String deviceRef, 
			String deviceModel, long duration) {
		super(EventType.CHANNEL_CHANGE_STB, customerRef, eventTime);
		this.channelRef = channelRef;
		this.deviceRef = deviceRef;
		this.deviceModel = deviceModel;
		this.duration = duration;
	}
	
}
