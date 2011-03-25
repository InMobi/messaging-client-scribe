package com.inmobi.messaging.netty;

import java.nio.ByteOrder;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;

import com.inmobi.messaging.Message;

/*
 * Emulation of Scribe RPC using TBinaryProtobol
 */
public class ScribeBites {
	private static final byte SINGLE_ENTRY_PREFIX[] = {
			(byte) 0x80, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 
			0x4c, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x00, 0x0f, 
			0x00, 0x01, 0x0c, 0x00, 0x00, 0x00, 0x01, 0x0b, 
			0x00, 0x01};
	private static final byte BODY_MARKER[] = {0x0b, 0x00, 0x02};
	private static final byte TRAILER[] = {0x00, 0x00};

	public static void publish(Channel ch, Message m) {
			ChannelBuffer output = ChannelBuffers.dynamicBuffer(ByteOrder.BIG_ENDIAN, 2048);
			
			output.writeBytes(SINGLE_ENTRY_PREFIX);
			
			byte[] catBytes = m.getTopic().getBytes();
			output.writeInt(catBytes.length);
			output.writeBytes(catBytes);

			output.writeBytes(BODY_MARKER);
			output.writeInt(m.getMessage().length);
			output.writeBytes(m.getMessage());
			
			output.writeBytes(TRAILER);
			ch.write(output);
	}
	
	public static void publish(Channel ch, ChannelBuffer categoryAsByteStream, byte stream[]) {
			ChannelBuffer output = ChannelBuffers.dynamicBuffer(ByteOrder.BIG_ENDIAN, 2048);
			
			output.writeBytes(categoryAsByteStream.duplicate());
			
			output.writeBytes(BODY_MARKER);
			output.writeInt(stream.length);
			output.writeBytes(stream);
			
			output.writeBytes(TRAILER);
			ch.write(output);
	}
	
	public static void publish(Channel ch, ChannelBuffer categoryAsByteStream, TBase thriftObject) throws TException {
		TNettyChannelBuffer t = new TNettyChannelBuffer(null, ChannelBuffers.dynamicBuffer());
		TProtocol p = new TBinaryProtocol(t);
		thriftObject.write(p);
		
		ChannelBuffer output = ChannelBuffers.dynamicBuffer(ByteOrder.BIG_ENDIAN, 2048);
		
		output.writeBytes(categoryAsByteStream.duplicate());
		
		output.writeBytes(BODY_MARKER);
		output.writeInt(t.getOutputBuffer().readableBytes());
		output.writeBytes(t.getOutputBuffer());
		
		output.writeBytes(TRAILER);
		ch.write(output);
	}
	
	public static ChannelBuffer generateHeaderWithCategory(String c) {
		ChannelBuffer categoryAsByteStream = ChannelBuffers.dynamicBuffer(ByteOrder.BIG_ENDIAN, c.length()+100);
		
		byte[] catBytes = c.getBytes();
		
		categoryAsByteStream.writeBytes(SINGLE_ENTRY_PREFIX);
		categoryAsByteStream.writeInt(catBytes.length);
		categoryAsByteStream.writeBytes(catBytes);
		
		return  ChannelBuffers.unmodifiableBuffer(categoryAsByteStream);
	}
}