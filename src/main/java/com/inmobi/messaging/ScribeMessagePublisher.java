package com.inmobi.messaging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import com.inmobi.instrumentation.TimingAccumulator;
import com.inmobi.messaging.netty.ScribeNettyImpl;

import scribe.thrift.LogEntry;
import com.inmobi.stats.EmitterRegistery;
import com.inmobi.stats.StatsEmitter;
import com.inmobi.stats.StatsExposer;



public class ScribeMessagePublisher extends AppenderSkeleton implements MessagePublisherMXBean {
    private static Charset charset = Charset.forName("ISO-8859-1");

    private String hostname = null;
    private int port = -1;
    private int backoffSeconds = 5;
    private int timeoutSeconds = 5;
    private String scribeCategory;
    private String emitterConfig = null;

    private ScribeNettyImpl publisher;

    private StatsEmitter emitter = null;
    private ScribeStats scribeStats = null;
    private Class<?> emClass = null;
    private Class<?> seInterface = null;


    private final class ScribeStats implements StatsExposer {

        private TimingAccumulator stats;
        private HashMap<String, String> contexts = new HashMap<String, String>();

        ScribeStats(TimingAccumulator s) {
            stats = s;
            // XXX
            // due to the inconsistency of the api, can't make sure category
            // is always set to sane value. This will work for the log4j appender
            // use case though.
            // - praddy
            contexts.put("category", getScribeCategory());
            contexts.put("scribe_type", "application");
        }

        public HashMap<String, Number> getStats() {
            HashMap<String, Number> hash = new HashMap<String, Number>();
            hash.put("cumulativeNanoseconds", stats.getCumulativeNanoseconds());
            hash.put("invocationCount", stats.getInvocationCount());
            hash.put("successCount", stats.getSuccessCount());
            hash.put("unhandledExceptionCount", stats.getUnhandledExceptionCount());
            hash.put("gracefulTerminates", stats.getGracefulTerminates());
            hash.put("inFlight", stats.getInFlight());
            return hash;
        }

        public HashMap<String, String> getContexts() {
            return contexts;
        }
    }


    public String getHostname() {
        return hostname;
    }

    public String getEmitterConfig() {
        return emitterConfig;
    }

    public void setEmitterConfig(String emitterConfig) {
        this.emitterConfig = emitterConfig;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getBackoffSeconds() {
        return backoffSeconds;
    }

    public void setBackoffSeconds(int backoffSeconds) {
        this.backoffSeconds = backoffSeconds;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public String getScribeCategory() {
        return scribeCategory;
    }

    public void setScribeCategory(String scribeCategory) {
        this.scribeCategory = scribeCategory;
    }

    public synchronized MessagePublisher build()
    {
        if(publisher ==  null)
        {
            publisher = new ScribeNettyImpl(hostname, port, timeoutSeconds, backoffSeconds);
        }
        if (emitterConfig != null && emitter == null) {
            try {
                emitter = EmitterRegistery.lookup(emitterConfig);
                scribeStats =  new ScribeStats(publisher.getStats());
                emitter.add(scribeStats);
                emitter.start();
            } catch (Exception e) {
                System.err.println("Couldn't find or initialize the stats emitter class");
                e.printStackTrace();
            }
        }
        return publisher;
    }

    @Override
    public void close()
    {
        if(publisher != null)
        {
            publisher.close();
        }
        if (emitter != null && scribeStats != null)
        {
            emitter.remove(scribeStats);
            emitter.stop();
        }
    }

    @Override
    public boolean requiresLayout()
    {
        return false;
    }

    @Override
    protected void append(LoggingEvent event)
    {
        Object o = event.getMessage();
        if( o instanceof TBase)
        {
            TBase thriftObject = (TBase) o;
            publisher.publish(thriftObject);
        } else if (o instanceof byte[]) {
            publisher.publish((byte[])o);
        } else if (o instanceof String) {
            publisher.publish(((String)o).getBytes());
        }
    }

    @Override
    public void activateOptions()
    {
        super.activateOptions();
        build();
        publisher.setFixedCategory(scribeCategory);
    }

    @Override
    public TimingAccumulator getStats() {
        return publisher == null ? null : publisher.getStats();
    }
}
