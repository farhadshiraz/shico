package com.shico.cassandra.statistic.domain;

public class WebtvLoginEvent extends StatEvent{
	public String deviceModel;
	public String webtvSessionId;
	public String webtvUsername;
	
	public WebtvLoginEvent(String customerRef,
			long eventTime, String deviceModel, String webtvSessionId,
			String webtvUsername) {
		super(EventType.WEBTV_LOGIN, customerRef, eventTime);
		this.deviceModel = deviceModel;
		this.webtvSessionId = webtvSessionId;
		this.webtvUsername = webtvUsername;
	}	
	
}
