package com.inmobi.messaging.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.instrumentation.TimingAccumulator.Outcome;
import com.inmobi.messaging.Message;
import com.inmobi.messaging.MessagePublisher;
import com.inmobi.messaging.MessagePublisherMXBean;

public class ScribeNettyImpl implements MessagePublisher, MessagePublisherMXBean {
    private static final Timer timer = new HashedWheelTimer();

    private final ClientBootstrap bootstrap;
    private volatile Channel ch = null;

    private final TimingAccumulator stats = new TimingAccumulator();


    private String host;
    private int port;
    private int backoff;

    private ChannelBuffer categoryAsByteStream;

    /**
     * This is meant to be a way for async callbacks to set the channel
     * on a successful connection
     *
     * Java does not have pointers to pointers. So have to resort to sending
     * in a wrapper object that knows to update our pointer
     */
    class ChannelSetter {
        public void setChannel(Channel ch) {
            Channel oldChannel = ScribeNettyImpl.this.ch;
            if(oldChannel != null && oldChannel.isOpen())
                oldChannel.close();
            ScribeNettyImpl.this.ch = ch;
        }
        public void connect() {bootstrap.connect(new InetSocketAddress(host, port));}
    }

    public ScribeNettyImpl(String hostname, int port, int timeoutSeconds,
                           int backoffSeconds) {
        bootstrap = new ClientBootstrap(NettyEventCore.getInstance().getFactory());

        ScribeHandler handler = new ScribeHandler(stats, new ChannelSetter(), backoffSeconds, timer);
        ChannelPipelineFactory cfactory = new ScribePipelineFactory(handler, timeoutSeconds, timer);

        bootstrap.setPipelineFactory(cfactory);

        this.host = hostname;
        this.port = port;
        this.backoff = backoffSeconds;

        bootstrap.connect(new InetSocketAddress(host, port));
    }

    @Override
    public void publish(Message m) {
        stats.accumulateInvocation();
        if(ch != null ) {
            ScribeBites.publish(ch, m);
        } else {
            suggestReconnect();
        }
    }

    public void publish(byte stream[]) {
        stats.accumulateInvocation();
        if(ch != null ) {
            ScribeBites.publish(ch, categoryAsByteStream, stream);
        } else {
            suggestReconnect();
        }
    }

    public void publish(TBase thriftObject) {
        stats.accumulateInvocation();
        try {
            ScribeBites.publish(ch, categoryAsByteStream, thriftObject);
        } catch (TException e) {
            stats.accumulateOutcomeWithDelta(Outcome.UNHANDLED_FAILURE, 0);
        }
    }

    private void suggestReconnect() {
        stats.accumulateOutcomeWithDelta(Outcome.UNHANDLED_FAILURE, 0);
        //TODO: logic for triggering reconnect
    }

    @Override
    public MessagePublisherMXBean getInspector() {
        return this;
    }

    @Override
    public TimingAccumulator getStats() {
        return stats;
    }

    public void close() {
        if(ch != null) {
            ch.close().awaitUninterruptibly();
        }
        NettyEventCore.getInstance().releaseFactory();
    }

    public synchronized boolean setFixedCategory(String c) {
        if(categoryAsByteStream  != null)
            return false;

        categoryAsByteStream = ScribeBites.generateHeaderWithCategory(c);

        return true;
    }
}