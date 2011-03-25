package com.inmobi.messaging.thrift;

import java.io.UnsupportedEncodingException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;

class TStringByteProtocol extends TBinaryProtocol {

	public TStringByteProtocol(TTransport trans) {
		super(trans);
	}

	@Override
	public void writeString(String str) throws TException {
	    try {
	        byte[] dat = str.getBytes("ISO-8859-1");
	        writeI32(dat.length);
	        trans_.write(dat, 0, dat.length);
	      } catch (UnsupportedEncodingException uex) {
	        throw new TException("JVM DOES NOT SUPPORT ISO-8859-1");
	      }
	}

	@Override
	public String readString() throws TException {
		throw new TException("Unimplemented");
	}
}
