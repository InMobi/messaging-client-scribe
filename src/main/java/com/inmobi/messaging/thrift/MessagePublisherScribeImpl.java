package com.inmobi.messaging.thrift;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransportException;

import scribe.thrift.LogEntry;
import scribe.thrift.ResultCode;
import scribe.thrift.scribe.AsyncClient;
import scribe.thrift.scribe.AsyncClient.Log_call;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.instrumentation.TimingAccumulator.Outcome;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.MessagePublisherMXBean;
import com.inmobi.messaging.MultiplexedMessagePublisher;

class MessagePublisherScribeImpl implements MultiplexedMessagePublisher, MessagePublisherMXBean {
	private static Charset charset = Charset.forName("ISO-8859-1");
	
	private static TAsyncClientManager selector;
	
	// A client get recycled on reconnect. Everyone needs to see this ASAP
	private volatile AsyncClient client;
	
	private Callback callback;
	
	private String host;
	private int port;
	private int backoff;
	private int timeout;

	private final Semaphore mutex = new Semaphore(1);
	
	private long lastConnectAttempt = -1;

	private final TimingAccumulator stats = new TimingAccumulator();


	private class Callback implements AsyncMethodCallback<Log_call>
	{
		@Override
		public void onComplete(Log_call response) {
			try {
				stats.accumulateOutcome(response.getResult() == ResultCode.OK ? Outcome.SUCCESS: Outcome.GRACEFUL_FAILURE, 0);
			} catch (TException e) {
				recordFailure();
			}
		}

		@Override
		public void onError(Throwable throwable) {
			recordFailure();
		}		
	}

	public MessagePublisherScribeImpl(String host, int port, int timeout, int backoff)
	{
		callback = new Callback();
		
		this.host = host;
		this.port = port;
		this.backoff = backoff;
		this.timeout = timeout * 1000;
		
		considerReconnect();
	}
	
	private void connect() throws TTransportException, IOException
	{
		AsyncClient newClient;
		
		/* Use sync not for thread safety but to ensure that all side effects,
		 * instruction re-ordering etc. etc. is done before the "client" variable
		 * is updated.
		 * 
		 * Doing this cleanly is required only during reconnects, and not the
		 * initial connect
		 */
		synchronized(this) {
			TNonblockingSocket socket = new TNonblockingSocket(host, port);
			socket.setTimeout(timeout);
			newClient = new scribe.thrift.scribe.AsyncClient(new TStringByteProtocol.Factory(), getSelector(), socket);
		}
		
		client = newClient; 
	}
	
	private void considerReconnect()
	{
		/*
		 *  Under heavy load and  with a borked connection, we need thread safety
		 *  
		 *  Every request to reconnect need not be honoured, we just need to try
		 *  once a while which under load, translates into "fast enough"
		 */
		
		if(mutex.tryAcquire()) {
			long currentTime = System.nanoTime();
				
			try {
				if( (currentTime - lastConnectAttempt) / 1e9 > backoff) {
					connect();					
				}
			} catch (TTransportException e) {
				stats.accumulateOutcome(Outcome.UNHANDLED_FAILURE, 0);
			} catch (IOException e) {
				stats.accumulateOutcome(Outcome.UNHANDLED_FAILURE, 0);
			} finally {
				lastConnectAttempt = currentTime;
				mutex.release();
			}
		}
	}

	static synchronized TAsyncClientManager getSelector() throws IOException
	{
		if(selector == null) {
			selector = new TAsyncClientManager();
		}
		return selector;
	}

	@Override
	public MessagePublisherMXBean getInspector() {
		return this;
	}
	
	private void recordFailure() {
		stats.accumulateOutcome(Outcome.UNHANDLED_FAILURE, 0);
		considerReconnect();
	}

	@Override
	public TimingAccumulator getStats() {
		return stats;
	}

	@Override
	public void publish(List<Message> l) {
		stats.accumulateInvocation();
		
		if(l == null)
			return;
		
		if(client == null) {
			//no connection? Try connecting once
			considerReconnect();

			// still no connection? tough luck
			if(client == null ) {
				recordFailure();
				return;
			}
		}
		
		List<LogEntry> ml = new ArrayList<LogEntry>(l.size());
		for(Message el: l) {
			Message bm = (Message) el;
			
			// Use ISO-8859-1 to ensure byte stream goes through unharmed?
			ml.add(new LogEntry().setCategory(bm.getTopic()).setMessage(new String(bm.getMessage(), charset)));
		}
		
		try {
			client.Log(ml, callback);
		} catch (TException e) {
			recordFailure();
		} catch (RuntimeException e) {
			recordFailure();
		}
	}

	public void scribePublish(List<LogEntry> l) {
		stats.accumulateInvocation();
		
		if(l == null)
			return;
		
		if(client == null) {
			//no connection? Try connecting once
			considerReconnect();

			// still no connection? tough luck
			if(client == null ) {
				recordFailure();
				return;
			}
		}
				
		try {
			client.Log(l, callback);
		} catch (TException e) {
			recordFailure();
		} catch (RuntimeException e) {
			recordFailure();
		}
	}

	@Override
	public void publish(Message m) {
		publish(Collections.singletonList(m));
	}
}