package com.intersystems.ingestion;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

public class UDP_XEP {

	public static void main(String[] args) {
			new UDPServer().start();
	}

}
