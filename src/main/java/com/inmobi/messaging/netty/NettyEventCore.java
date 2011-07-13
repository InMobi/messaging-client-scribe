package com.inmobi.messaging.netty;

import org.jboss.netty.channel.socket.*;
import org.jboss.netty.channel.socket.nio.*;

import java.util.concurrent.*;

class NettyEventCore {
    private static final NettyEventCore ourInstance = new NettyEventCore();

    private ClientSocketChannelFactory factory = null;
    private int leases = 0;

    public static NettyEventCore getInstance() {
        return ourInstance;
    }

    private NettyEventCore() {
    }

    /**
     * Get a handle to the netty event loop
     *
     * The NIO handlers are setup lazily
     * @return an NIO handler
     */
    public synchronized ClientSocketChannelFactory getFactory() {
        if(factory == null) {
            factory = new NioClientSocketChannelFactory(
                    Executors.newCachedThreadPool(),
                    Executors.newCachedThreadPool());
        }
        leases++;
        return factory;
    }

    /**
     * Application indicating that it no longer needs this
     *
     * In real world, assume that this will not be released,
     * we are being nice and try and release it.
     */
    public synchronized void releaseFactory() {
        if(factory != null) {
            leases--;
            if(leases == 0) {
                factory.releaseExternalResources();
                factory = null;
            }
        } else {
            //WTF! releasing what you did not take
        }
    }
}