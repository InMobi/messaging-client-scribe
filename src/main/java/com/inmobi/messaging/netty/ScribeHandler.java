package com.inmobi.messaging.netty;

import java.net.ConnectException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.instrumentation.TimingAccumulator.Outcome;
import com.inmobi.messaging.netty.ScribeNettyImpl.ChannelSetter;

public class ScribeHandler extends SimpleChannelHandler {
	private final TimingAccumulator stats;
	private final ChannelSetter channelSetter;
	private volatile long connectRequestTime = 0;
	private long backoffSeconds;
	private Timer timer;
	
	private final Semaphore lock = new Semaphore(1);
	
	public ScribeHandler(TimingAccumulator stats, ChannelSetter channelSetter, int backoffSeconds, Timer timer) {
		this.stats = stats;
		this.channelSetter = channelSetter;
		this.backoffSeconds = backoffSeconds;
		this.timer = timer;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		
		ChannelBuffer buf = (ChannelBuffer) e.getMessage();
		//TODO: get a grip on response parsing
		stats.accumulateOutcomeWithDelta(Outcome.SUCCESS, 0);
		
		
//		if(buf.readableBytes() == 20) {
//			stats.accumulateOutcomeWithDelta(buf.getByte(18) == 0x00 ? Outcome.SUCCESS : Outcome.GRACEFUL_FAILURE, 0);
//		} else {
//			/*
//			 * TODO:
//			 * WTF! Scribe response is always 23 bytes
//			 * I Know TCP is stream protocol and all but no 23 bytes?
//			 */
//			stats.accumulateOutcomeWithDelta(Outcome.UNHANDLED_FAILURE, 0);
//		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		Throwable cause = e.getCause();
		
		
		if(!(cause instanceof ConnectException)) {
			stats.accumulateOutcomeWithDelta(Outcome.UNHANDLED_FAILURE, 0);
		} 
		scheduleReconnect();
		
		//bubble to enable default logging. This is really stoopid way
		super.exceptionCaught(ctx, e);
	}
	
	private void scheduleReconnect() {
		/*
		 *  Ensure you are the only one mucking with connections
		 *  If you find someone else is doing so, then you don't get a turn
		 *  We trust this other person to do the needful
		 */
		if(lock.tryAcquire()) {
			long currentTime = System.currentTimeMillis();        		
			// Check how long it has been since we reconnected
			try {
				if((currentTime - connectRequestTime)/1000 > backoffSeconds) {
					connectRequestTime = currentTime;
			        timer.newTimeout(new TimerTask() {
			            public void run(Timeout timeout) throws Exception {
			            	channelSetter.connect();
			            }}, backoffSeconds, TimeUnit.SECONDS);
				}
			} finally {
				lock.release();
			}
		}
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		lock.acquire();
		try {
			channelSetter.setChannel(e.getChannel());
		} finally {
			lock.release();
		}
	}
}