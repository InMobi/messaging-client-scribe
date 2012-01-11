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
//import com.inmobi.stats.StatsExposer;
//import com.inmobi.stats.EmitMondemand;



public class ScribeMessagePublisher extends AppenderSkeleton implements MessagePublisherMXBean {
    private static Charset charset = Charset.forName("ISO-8859-1");

    private String hostname = null;
    private int port = -1;
    private int backoffSeconds = 5;
    private int timeoutSeconds = 5;
    private String scribeCategory;
    private boolean emitStats;

    private ScribeNettyImpl publisher;

    private Object emitter = null;
    private Class<?> emClass = null;
    private Class<?> seInterface = null;


    public String getHostname() {
        return hostname;
    }

    public boolean getEmitStats() {
        return emitStats;
    }

    public void setEmitStats(boolean emitStats) {
        this.emitStats = emitStats;
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
        if (emitStats && emitter == null) {
            try {
                ClassLoader cl = this.getClass().getClassLoader();
                emClass = cl.loadClass("com.inmobi.stats.emitter.EmitMondemand");
                seInterface = cl.loadClass("com.inmobi.stats.StatsExposer");

                class ScribeStats {

                    private TimingAccumulator stats;

                    ScribeStats(TimingAccumulator s) {
                        stats = s;
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
                }

                class ScribeStatsProxy implements InvocationHandler {

                    private ScribeStats proxee = null;

                    ScribeStatsProxy (ScribeStats s) {
                        proxee = s;
                    }

                    public Object invoke(Object proxy, Method method, Object[] args) {
                        try {
                            Method m = proxee.getClass().getDeclaredMethod(
                                    method.getName(),
                                    method.getParameterTypes());
                            return m.invoke(proxee, args);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return null;
                        }
                    }
                }

                HashMap<String, String> contexts = new HashMap<String, String>();
                // XXX
                // due to the inconsistency of the api, can't make sure category
                // is always set to sane value. This will work for the log4j appender
                // use case though.
                // - praddy
                contexts.put("category", getScribeCategory());
                contexts.put("scribe_type", "application");
                Class<?>[] proto = {seInterface, String.class, HashMap.class};
                ScribeStats s = new ScribeStats(publisher.getStats());
                Object proxy = Proxy.newProxyInstance(
                        s.getClass().getClassLoader(),
                        new Class[] {seInterface},
                        new ScribeStatsProxy(s));
                Object[] params = {seInterface.cast(proxy), "scribe", contexts};
                try {
                    emitter = emClass.getConstructor(proto).newInstance(params);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                //emitter = new EmitMondemand(new ScribeStats(publisher.getStats()), "scribe", contexts);
            } catch (ClassNotFoundException e) {
                System.err.println("emitStats is set to true but couldn't load the following classes\nplease make sure that com.inmobi.stats is in your classpath");
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
        if (emitter != null)
        {
            Class<?>[] proto = {};
            Object[] params = {};
            try {
                emClass.getMethod("stop", proto).invoke(emitter, params);
            } catch (Exception e) {
                e.printStackTrace();
            }
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
        } else {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(o);
                oos.flush();
                oos.close();
                baos.close();
                publisher.publish(baos.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
            }
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
