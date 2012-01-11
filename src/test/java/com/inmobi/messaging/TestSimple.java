package com.inmobi.messaging;

import static org.testng.Assert.assertEquals;

import org.apache.thrift.TException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import random.pkg.NtMultiServer;
import random.pkg.ScribeAlwaysTryAgain;

import com.inmobi.instrumentation.TimingAccumulator;

public class TestSimple {
	private NtMultiServer server;
	private ScribeMessagePublisher mb;
	
	@BeforeTest
	public void setUp() {
		server = TestServerStarter.getServer();
	}
	
	@AfterTest
	public void tearDown()
	{
		server.stop();
		if(mb != null)
			mb.close();
	}
	
	@Test()
	public void simpleSend() throws TException, InterruptedException
	{
		server.start();

		mb = new ScribeMessagePublisher();
		mb.setHostname("localhost");
		mb.setPort(TestServerStarter.port);
		MessagePublisher m = mb.build();
		waitForConnectComplete(m);
		TimingAccumulator inspector = m.getInspector().getStats();
		
		long success = inspector.getSuccessCount();
		m.publish( new Message("ch", "mmmm".getBytes()) );

		//Wait for all operations to complete
		while(inspector.getInFlight() != 0)
		{
			Thread.sleep(100);
		}
		assertEquals(inspector.getSuccessCount(), success + 1);
	}

	@Test(timeOut = 100000)
	public void serialBlaster() throws TException, InterruptedException
	{
		final int loop = 100*1000;
		
		server.start();

		mb = new ScribeMessagePublisher();
		mb.setHostname("localhost");
		mb.setPort(TestServerStarter.port);
		MessagePublisher m = mb.build();
		waitForConnectComplete(m);
				
		TimingAccumulator inspector = m.getInspector().getStats();
		
		long success = inspector.getSuccessCount();
		
		for(int i = 0; i < loop; i++)
		{
			m.publish( new Message("ch", ("mmmm" + i).getBytes()) );
		}

		//Wait for all operations to complete
		while(inspector.getInFlight() != 0)
		{
			Thread.sleep(100);
		}
		
		assertEquals(inspector.getSuccessCount(), success + loop);
	}

	@Test(timeOut = 100000)
	public void throttledSerialBlaster() throws TException, InterruptedException
	{
		final int loop = 100*1000;
		
		server.start();

		mb = new ScribeMessagePublisher();
		mb.setHostname("localhost");
		mb.setPort(TestServerStarter.port);
		MessagePublisher m = mb.build();
		waitForConnectComplete(m);
				
		TimingAccumulator inspector = m.getInspector().getStats();
		
		long success = inspector.getSuccessCount();
		
		for(int i = 0; i < loop; i++)
		{
			m.publish( new Message("ch", ("mmmm" + i).getBytes()) );
			if( (i & 0x3fff) == 0)
			{
				System.out.println("pacing after " + i);
				Thread.sleep(60);
			}
		}

		//Wait for all operations to complete
		while(inspector.getInFlight() != 0)
		{
			Thread.sleep(100);
		}
		
		assertEquals(inspector.getSuccessCount(), success + loop);
	}

	// @Test(timeOut = 10000)
	@Test(timeOut = 100000)
	public void concurrentSend() throws TException, InterruptedException
	{
		final int loop = 1000;
		final int threadCount = 100;

		Thread t[] = new Thread[threadCount];
		
		server.start();

		mb = new ScribeMessagePublisher();
		mb.setHostname("localhost");
		mb.setPort(TestServerStarter.port);
		final MessagePublisher m = mb.build();
		waitForConnectComplete(m);
				
		TimingAccumulator inspector = m.getInspector().getStats();
		
		long success = inspector.getSuccessCount();
		
		for(int i = 0; i < threadCount; i++)
		{
			t[i] = new Thread()
			{
				@Override
				public void run()
				{
					for(int i = 0; i < loop; i++)
					{
						m.publish( new Message("ch", ("mmmm" + i + Thread.currentThread().getId()).getBytes()) );
					}
				}
			};
		}

		for(int i = 0; i < threadCount; i++)
		{
			t[i].start();
		}

		for(int i = 0; i < threadCount; i++)
		{
			t[i].join();
		}
		
		//Wait for all operations to complete
		while(inspector.getInFlight() != 0)
		{
			Thread.sleep(100);
		}
		
		assertEquals(inspector.getSuccessCount(), success + loop*threadCount);
	}

	@Test()
	public void overwhelmedServer() throws TException, InterruptedException
	{
		NtMultiServer tserver = null;
		try {
			int port = 7913;
			tserver = new NtMultiServer(new ScribeAlwaysTryAgain(), port);
			tserver.start();
	
			mb = new ScribeMessagePublisher();
			mb.setHostname("localhost");
			mb.setPort(port);
			MessagePublisher m = mb.build();
			waitForConnectComplete(m);
			TimingAccumulator inspector = m.getInspector().getStats();
			
			long c = inspector.getGracefulTerminates();
			m.publish( new Message("ch", "mmmm".getBytes()) );
	
			//Wait for all operations to complete
			while(inspector.getInFlight() != 0)
			{
				Thread.sleep(100);
			}
			assertEquals(inspector.getGracefulTerminates(), c + 1);
		} finally {
			tserver.stop();
		}
	}

	public static void waitForConnectComplete(MessagePublisher m) {
		// Stoopid implementation
		try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
