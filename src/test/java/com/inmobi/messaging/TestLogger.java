package com.inmobi.messaging;

import static org.testng.Assert.assertEquals;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.instrumentation.TimingAccumulator;

import random.pkg.NtMultiServer;
import scribe.thrift.LogEntry;

public class TestLogger {
	private NtMultiServer server;
	
	@BeforeTest
	public void setUp() {
		server = TestServerStarter.getServer();
	}

	@AfterTest
	public void tearDown()
	{
		server.stop();
	}
	
	@Test
	public void log() throws InterruptedException
    {
        server.start();

        PropertyConfigurator.configure("src/test/resources/log-four-jay.properties");

        Logger l = Logger.getLogger("localscribe");
        TimingAccumulator inspector = ((ScribeMessagePublisher)l.getAppender("SA2")).getStats();

        LogEntry le = new LogEntry();
        le.category="xxxx";
        le.message="massage";

        long success = inspector.getSuccessCount();

        l.fatal(le);

        //Wait for all operations to complete
        while(inspector.getInFlight() != 0)
        {
            Thread.sleep(1000);
        }
        assertEquals(inspector.getSuccessCount(), success + 1);
    }
}
