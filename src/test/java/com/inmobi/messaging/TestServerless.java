package com.inmobi.messaging;

import static org.testng.Assert.assertEquals;

import org.apache.thrift.TException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import random.pkg.NtMultiServer;

import com.inmobi.instrumentation.TimingAccumulator;

public class TestServerless {
	private NtMultiServer server;
	private ScribeMessagePublisher mb;
	
	@BeforeTest
	public void setUp() {
		server = TestServerStarter.getServer();
	}
	
	@AfterTest
	public void tearDown()
	{
		if(mb != null)
			mb.close();
	}
	
	@Test()
	public void sendWithoutServer() throws TException, InterruptedException
	{
		server.stop();

		mb = new ScribeMessagePublisher();
		mb.setHostname("localhost");
		mb.setPort(TestServerStarter.port);
		MessagePublisher m = mb.build();
				
		TimingAccumulator inspector = m.getInspector().getStats();
		
		long failure = inspector.getUnhandledExceptionCount();
			
		m.publish( new Message("ch", "mmmm".getBytes()) );
		
		//Wait for all operations to complete
		while(inspector.getInFlight() != 0)
		{
			Thread.sleep(100);
		}
		
		assertEquals(inspector.getUnhandledExceptionCount(), failure + 1);
	}

	
	@Test()
	public void serverGone() throws TException, InterruptedException
	{
		server.start();

		mb = new ScribeMessagePublisher();
		mb.setHostname("localhost");
		mb.setPort(TestServerStarter.port);
		MessagePublisher m = mb.build();
		Thread.sleep(1000);
		TimingAccumulator inspector = m.getInspector().getStats();
		
		long success = inspector.getSuccessCount();
		long failure = inspector.getUnhandledExceptionCount();
		m.publish( new Message("ch", "mmmm".getBytes()) );

		//Wait for all operations to complete
		while(inspector.getInFlight() != 0)
		{
			Thread.sleep(100);
		}
		assertEquals(inspector.getSuccessCount(), success + 1);
		
		// Now stop the server
		server.stop();
		Thread.sleep(100);

		m.publish( new Message("ch", "mmmm".getBytes()) );
		
		//Wait for all operations to complete
		while(inspector.getInFlight() != 0)
		{
			Thread.sleep(100);
		}		
		assertEquals(inspector.getUnhandledExceptionCount(), failure + 1);
	}
}