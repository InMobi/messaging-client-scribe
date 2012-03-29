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
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import scribe.thrift.ResultCode;

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
		
        ResultCode success;
		ChannelBuffer buf = (ChannelBuffer) e.getMessage();
        TMemoryInputTransport trans = new TMemoryInputTransport(buf.array());
        TBinaryProtocol proto = new TBinaryProtocol(trans);
        TMessage msg = proto.readMessageBegin();
        if (msg.type == TMessageType.EXCEPTION) {
            proto.readMessageEnd();
        }
        TField field;
        proto.readStructBegin();
        while (true)
        {
            field = proto.readFieldBegin();
            if (field.type == TType.STOP) {
                break;
            }
            switch (field.id) {
                case 0: // SUCCESS
                    if (field.type == TType.I32) {
                        success = ResultCode.findByValue(proto.readI32());
                        stats.accumulateOutcomeWithDelta(success.getValue() == 0 ? Outcome.SUCCESS : Outcome.GRACEFUL_FAILURE, 0);
                    } else {
                        TProtocolUtil.skip(proto, field.type);
                    }
                    break;
                default:
                    TProtocolUtil.skip(proto, field.type);
            }
            proto.readFieldEnd();
        }
        proto.readStructEnd();
        proto.readMessageEnd();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		Throwable cause = e.getCause();
		
		
		if(!(cause instanceof ConnectException)) {
			stats.accumulateOutcomeWithDelta(Outcome.UNHANDLED_FAILURE, 0);
		} 
		scheduleReconnect();
		
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
