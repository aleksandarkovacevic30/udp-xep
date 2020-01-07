package com.intersystems.ingestion;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.sql.Timestamp;

public class Test {
	private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	    SensorData c1 = new SensorData ();
	    c1.time=new Timestamp(System.currentTimeMillis());
	    c1.Value1=0.1234;
	    c1.Value2=1.1235;
	    c1.Value3=2.1236;
	    c1.Value4=3.1237;
	    c1.Value5=4.1238;
	    c1.Value6=5.1239;
	    c1.Value7=6.1240;
	    try {
	        ByteArrayOutputStream baos = new ByteArrayOutputStream();
	      ObjectOutputStream oos = new ObjectOutputStream(baos);
	      oos.writeObject(c1);
	      oos.flush();
	      // get the byte array of the object
	      byte[] Buf= baos.toByteArray();
	      System.out.println(bytesToHex(Buf));
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	 
	}
	public static String bytesToHex(byte[] bytes) {
	    char[] hexChars = new char[bytes.length * 2];
	    for (int j = 0; j < bytes.length; j++) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 2] = HEX_ARRAY[v >>> 4];
	        hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
	    }
	    return new String(hexChars);
	}

}
