package com.inmobi.messaging.netty;

import static org.jboss.netty.channel.Channels.pipeline;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.Timer;

public class ScribePipelineFactory implements ChannelPipelineFactory {
    private final ScribeHandler handler;
    private final int MAX_FRAME_SIZE = 512 * 1024;
	private final int timeout;
	private final Timer timer;
    
    public ScribePipelineFactory(ScribeHandler handler, int socketTimeout, Timer timer) {
    	this.handler = handler;
    	this.timeout = socketTimeout;
    	this.timer = timer;
    }

    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = pipeline();
        pipeline.addLast("timeout", new ReadTimeoutHandler(timer, timeout));
        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(MAX_FRAME_SIZE, 0, 4, 0, 4));
        pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
        pipeline.addLast("thriftHandler", handler);
        return pipeline;
    }

}