package com.inmobi.messaging;

import java.nio.charset.Charset;
import java.util.Collections;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.netty.ScribeNettyImpl;

import scribe.thrift.LogEntry;


public class ScribeMessagePublisher extends AppenderSkeleton implements MessagePublisherMXBean {
	private static Charset charset = Charset.forName("ISO-8859-1");
	
	private String hostname = null;
	private int port = -1;
	private int backoffSeconds = 5;
	private int timeoutSeconds = 5;
	private String scribeCategory;
	
	private ScribeNettyImpl publisher;

	
	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getBackoffSeconds() {
		return backoffSeconds;
	}

	public void setBackoffSeconds(int backoffSeconds) {
		this.backoffSeconds = backoffSeconds;
	}

	public int getTimeoutSeconds() {
		return timeoutSeconds;
	}

	public void setTimeoutSeconds(int timeoutSeconds) {
		this.timeoutSeconds = timeoutSeconds;
	}

	public String getScribeCategory() {
		return scribeCategory;
	}

	public void setScribeCategory(String scribeCategory) {
		this.scribeCategory = scribeCategory;
	}

	public synchronized MessagePublisher build()
	{
		if(publisher ==  null)
		{
			publisher = new ScribeNettyImpl(hostname, port, timeoutSeconds, backoffSeconds);
		}
		return publisher;
	}
	
	@Override
	public void close()
	{
		if(publisher != null)
		{
			publisher.close();
		}
	}

	@Override
	public boolean requiresLayout()
	{
		return false;
	}

	@Override
	protected void append(LoggingEvent event)
	{
		Object o = event.getMessage();
		if( o instanceof TBase)
		{
			TBase thriftObject = (TBase) o;
			publisher.publish(thriftObject);
		}
	}

	@Override
	public void activateOptions()
	{
		super.activateOptions();
		build();
		publisher.setFixedCategory(scribeCategory);
	}

	@Override
	public TimingAccumulator getStats() {
		return publisher == null ? null : publisher.getStats();
	}
}