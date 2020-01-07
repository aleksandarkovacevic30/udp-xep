package com.intersystems.ingestion;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Timestamp;

public class UDPServer extends Thread {
 
	public static final Integer RECEIVE_SOCKET_SIZE=1024*1024*512;
	public static final Integer TEMPORARY_BUFFER_SIZE=64;
	public static final Integer MEASUREMENT_BATCH=16384;
	
    private DatagramSocket socket;
    private boolean running;
    private byte[] buf = new byte[TEMPORARY_BUFFER_SIZE];
    private XEP_Persister xep;
    ByteBuffer bb; 
    private SensorData[] measurements = new SensorData[MEASUREMENT_BATCH];
    private int measPointer=0;
    
    public UDPServer() {
    	xep = new XEP_Persister();
        try {
			socket = new DatagramSocket(4445);
			socket.setReceiveBufferSize(RECEIVE_SOCKET_SIZE); 
			bb = ByteBuffer.allocate(TEMPORARY_BUFFER_SIZE);
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
 
    public void run() {
        running = true;
        int i=0;
        while (running) {
            DatagramPacket packet 
              = new DatagramPacket(buf, buf.length);
            
            try {
				socket.receive(packet);
				if (++i%1000000 == 0) {
					System.out.println(i);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
            	bb.put(packet.getData());
            	bb.flip();
				SensorData current=convertFromBytes(bb);
				bb.clear();
				measurements[measPointer]=current;
				measPointer++;
				if (measPointer==MEASUREMENT_BATCH) {
					Long time= xep.XEPSaveSensorData(measurements);
					System.out.println("saved it within "+time+" miliseconds");
					measPointer=0;
				}
			
            
            
        }
        socket.close();
        try {
			xep.closeXEP();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    private SensorData convertFromBytes(ByteBuffer buf) {
    	SensorData result=new SensorData();
    	result.time=new Timestamp(buf.getLong(0));
    	result.Value1=buf.getDouble(8);
    	result.Value2=buf.getDouble(8*2);
    	result.Value3=buf.getDouble(8*3);
    	result.Value4=buf.getDouble(8*4);
    	result.Value5=buf.getDouble(8*5);
    	result.Value6=buf.getDouble(8*6);
    	result.Value7=buf.getDouble(8*7);
    	return result;
    }
}