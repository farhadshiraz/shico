package com.shico.cassandra.statistic.domain;

public class StatEvent {
	public EventType eventType;
	public String customerRef;
	public long eventTime;
	
	public StatEvent(EventType eventType, String customerRef, long eventTime) {
		super();
		this.eventType = eventType;
		this.customerRef = customerRef;
		this.eventTime = eventTime;
	}
}
