package com.inmobi.messaging;

import static org.testng.Assert.assertNotNull;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import random.pkg.NtMultiServer;

public class TestServerStarter {
	private static NtMultiServer server;

	@BeforeSuite
	public void setUp()
	{
		safeInit();
	}

	@AfterSuite
	public void tearDown()
	{
		server.stop();
	}
	
	public static NtMultiServer getServer()
	{
		safeInit();
		assertNotNull(server);
		return server;
	}

	private static synchronized void safeInit()
	{
		if(server == null)
		{
			server = new NtMultiServer();	
		}
	}
	
	public static final int port = 7912;
	
}
