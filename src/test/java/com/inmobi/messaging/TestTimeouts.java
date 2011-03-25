package com.inmobi.messaging;

import static org.testng.Assert.assertEquals;

import org.apache.thrift.TException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import random.pkg.NtMultiServer;
import random.pkg.ScribeSlacker;

import com.inmobi.instrumentation.TimingAccumulator;

public class TestTimeouts {
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
		NtMultiServer tserver = null;
		try {
			int port = 7914;
			tserver = new NtMultiServer(new ScribeSlacker(), port);
			tserver.start();
	
			mb = new ScribeMessagePublisher();
			mb.setHostname("localhost");
			mb.setPort(port);
			int timeoutSeconds = 2;
			mb.setTimeoutSeconds(timeoutSeconds);
			MessagePublisher m = mb.build();
			TestSimple.waitForConnectComplete(m);
			TimingAccumulator inspector = m.getInspector().getStats();
			
			long error = inspector.getUnhandledExceptionCount();
			m.publish( new Message("ch", "mmmm".getBytes()) );
	
			Thread.sleep((timeoutSeconds + 1) * 1000);
			assertEquals(inspector.getInFlight(), 0, "ensure not considered midflight");
			assertEquals(inspector.getUnhandledExceptionCount(), error + 1, "check if recorded as error");
		} finally {
			tserver.stop();
		}
	}
}