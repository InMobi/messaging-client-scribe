package random.pkg;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import scribe.thrift.scribe;
import scribe.thrift.scribe.Iface;

public class SimpleSocketServer {
	private int port = 7911;
	private TServerTransport serverTransport;
	private TServer server;
	private Iface handler;
	
	private Thread t;
	
	public SimpleSocketServer(int port) {
		this(port, new ScribeAlwaysSuccess());
	}
	
	public SimpleSocketServer(int port, Iface handler) {
		this.handler = handler;
		this.port = port;
	}
	
	public synchronized void start() throws TTransportException {
		if(server != null)
			return;
		
		serverTransport = new TServerSocket(port);
		scribe.Processor processor = new scribe.Processor(handler);
		TProtocolFactory protFactory = new TBinaryProtocol.Factory(true, true);
		server = new TThreadPoolServer(processor, serverTransport,
				new TFramedTransport.Factory(), protFactory);
		t = new Thread() {
			public void run(){
				server.serve();
			}
		};
		t.start();
	}
	
	public synchronized void stop() {
		if(server != null)
		{
			server.stop();
			serverTransport.close();
			server = null;
			serverTransport = null;
			t.stop();
		}
	}

}
