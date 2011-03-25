package com.inmobi.messaging.thrift;

import static org.testng.Assert.assertEquals;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Random;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.testng.annotations.Test;

import com.inmobi.messaging.thrift.TStringByteProtocol;

import scribe.thrift.LogEntry;

public class TestTStringByteProtocol {

	@Test
	public void tryAllBytes() throws TException {
		int range = Byte.MAX_VALUE - Byte.MIN_VALUE + 1;
		
		byte in[] = new byte[range];
		int j;
		byte i;
		
		for(i = Byte.MIN_VALUE,  j =0; j < range ; i++,j++) {
			in[j] = i;
		}
		
		tryBytes(in);
	}

	@Test
	public void tryRandomBytes() throws TException {
		Random r = new Random();
		
		for(int i =0; i < 10000; i++) {
			byte in[] = new byte[(short)r.nextInt(Short.MAX_VALUE)];
			int j;
			for(j =0; j < in.length ; j++) {
				in[j] = (byte)r.nextInt();
			}
			
			tryBytes(in);
		}
	}
	
	private void tryBytes(byte[] in) throws TException {		
		LogEntry m = new LogEntry();
		m.category = "foo";
		Charset charset = Charset.forName("ISO-8859-1");
		m.message = new String(in, charset);
		
		TMemoryBuffer tmb = new TMemoryBuffer(in.length);
		TProtocol p = new TStringByteProtocol(tmb);
		m.write(p);
		
		byte out[] = tmb.getArray();
		int l = tmb.length();
		
		byte res[] = Arrays.copyOfRange(out, l-in.length-1, l-1);
		
		assertEquals(in, res);
	}
}
