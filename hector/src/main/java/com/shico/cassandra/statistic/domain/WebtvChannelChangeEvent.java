package com.shico.cassandra.statistic.domain;

public class WebtvChannelChangeEvent extends ChannelChangeEvent {
	public String webtvSessionId;
	public String webtvUsername;
	
	public WebtvChannelChangeEvent(String customerRef,
			long eventTime, String channelRef,
			String deviceModel, long duration,
			String webtvSessionId, String webtvUsername) {
		super(customerRef, eventTime, channelRef, null,
				deviceModel, duration);
		super.eventType = EventType.CHANNEL_CHANGE_WEBTV;
		this.webtvSessionId = webtvSessionId;
		this.webtvUsername = webtvUsername;
	}
}
