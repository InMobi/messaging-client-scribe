package com.inmobi.messaging;

import static org.testng.Assert.assertEquals;

import org.apache.thrift.TException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.instrumentation.TimingAccumulator;

import random.pkg.NtMultiServer;

public class TestReconnect {
	private NtMultiServer server;
	
	@BeforeTest
	public void setUp() {
		server = TestServerStarter.getServer();
	}

	@Test(timeOut = 5000)
	public void serverShowsUpLate() throws TException, InterruptedException
	{
		server.stop();
		
		int backoffSeconds = 3;

		ScribeMessagePublisher mb = new ScribeMessagePublisher();
		mb.setHostname("localhost");
		mb.setPort(TestServerStarter.port);
		mb.setBackoffSeconds(backoffSeconds);
		MessagePublisher m = mb.build();
		TestSimple.waitForConnectComplete(m);
				
		TimingAccumulator inspector = m.getInspector().getStats();
		
		long success = inspector.getSuccessCount();
		
		server.start();
		Thread.sleep( (backoffSeconds + 1) * 1000);
		TestSimple.waitForConnectComplete(m);
		
		m.publish( new Message("ch", "mmmm".getBytes()) );
		
		//Wait for all operations to complete
		while(inspector.getInFlight() != 0)
		{
			Thread.sleep(100);
		}

		assertEquals(inspector.getSuccessCount(), success + 1);
	}

	@Test(timeOut = 10000)
	public void simpleReconnect() throws TException, InterruptedException
	{
		server.start();
		
		int backoffSeconds = 3;

		ScribeMessagePublisher mb = new ScribeMessagePublisher();
		mb.setHostname("localhost");
		mb.setPort(TestServerStarter.port);
		mb.setBackoffSeconds(backoffSeconds);
		mb.setTimeoutSeconds(1000000);
		MessagePublisher m = mb.build();
		TestSimple.waitForConnectComplete(m);
				
		TimingAccumulator inspector = m.getInspector().getStats();
		
		long success = inspector.getSuccessCount();
		long failure = inspector.getUnhandledExceptionCount();

		// 1 success
		m.publish( new Message("ch", "mmmm".getBytes()) );
		//Wait for all operations to complete
		while(inspector.getInFlight() != 0)
		{
			Thread.sleep(100);
		}

		server.stop();
		
		// 1 failure
		m.publish( new Message("ch", "mmmm".getBytes()) );
		while(inspector.getInFlight() != 0)
		{
			Thread.sleep(100);
		}
		
		server.start();
		Thread.sleep( (backoffSeconds + 1) * 1000);
				
		// We should have new connection by now
		m.publish( new Message("ch", "mmmm".getBytes()) );
		//Wait for all operations to complete
		while(inspector.getInFlight() != 0)
		{
			Thread.sleep(100);
		}

		
		assertEquals(inspector.getSuccessCount(), success + 2);
		assertEquals(inspector.getUnhandledExceptionCount(), failure + 1);
	}
}