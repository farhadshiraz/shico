package com.shico.cassandra.statistic.domain;

public class WidgetActivationEvent extends StatEvent {
	public String deviceRef;
	public String widget;
	
	public WidgetActivationEvent(String customerRef,
			long eventTime, String deviceRef, String widget) {
		super(EventType.WIDGET_ACTIVATION, customerRef, eventTime);
		this.deviceRef = deviceRef;
		this.widget = widget;
	}
	
}
