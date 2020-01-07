package com.intersystems.ingestion;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashMap;

import com.intersystems.xep.Event;
import com.intersystems.xep.EventPersister;
import com.intersystems.xep.EventQuery;
import com.intersystems.xep.EventQueryIterator;
import com.intersystems.xep.PersisterFactory;
import com.intersystems.xep.XEPException;

import com.intersystems.ingestion.SensorData;

//import instrumentation.Agent;

public class XEP_Persister {
	
	private EventPersister xepPersister;
	private Event xepEvent;
	public XEP_Persister() {
        HashMap<String, String> map = new HashMap<String, String>();
        try{
            map = getConfig("config.txt");
        }
        catch (IOException e){
            System.out.println(e.getMessage());
        }

        String ip = map.get("ip");
        int port = Integer.parseInt(map.get("port"));
        String namespace = map.get("namespace");
        String username = map.get("username");
        String password = map.get("password");
        
	    try {
	    	// Initialize sampleArray to hold SensorData items
	    	SensorData[] sampleArray = null;
	    	
	    	// Connect to database using EventPersister, which is based on IRISDataSource
	        xepPersister = PersisterFactory.createPersister();

	        // Connecting to database
	        xepPersister.connect(ip, port, namespace, username, password);
			System.out.println("Connected to InterSystems IRIS.");

	        xepPersister.deleteExtent("com.intersystems.ingestion.SensorData");   // Remove old test data
	        xepPersister.importSchema("com.intersystems.ingestion.SensorData");   // Import flat schema
	       
	        // Create Event
	        xepEvent = xepPersister.getEvent("com.intersystems.ingestion.SensorData");

		} catch (XEPException e) {
			/*
			 * TODO handle exceptions
			 * */
			System.out.println("Interactive prompt failed:\n" + e); 

	}
	}
	    public void closeXEP() throws SQLException {
	        xepEvent.close();
	        xepPersister.close();
	    }
	// Save array of SensorData into database using xepEvent
	public Long XEPSaveSensorData(SensorData[] sampleArray)
	{
		Long startTime = System.currentTimeMillis(); // To calculate execution time
		xepEvent.store(sampleArray);
		Long totalTime = System.currentTimeMillis() - startTime;
		System.out.println("Saved " + sampleArray.length + " SensorData(s).");
		return totalTime;
	}

	// Iterate over all SensorDatas
	public static Long ViewAll(Event xepEvent)
	{
		//Create and execute query using EventQuery
		String sqlQuery = "SELECT * FROM Solutions_Demo.SensorData WHERE purchaseprice > ? ORDER BY stockname, purchaseDate"; 
		EventQuery<SensorData> xepQuery = xepEvent.createQuery(sqlQuery);
		xepQuery.setParameter(1,"0");    // find stocks purchased > $0/share (all)
		Long startTime = System.currentTimeMillis();
		xepQuery.execute();
		
		Integer cnt=0;
		Long size=0L;
		// Iterate through and write names of stocks using EventQueryIterator
		EventQueryIterator<SensorData> xepIter = xepQuery.getIterator();
		while (xepIter.hasNext()) {
		  SensorData newSample = xepIter.next();
//		  newSample.stockName = "NYSE-" + newSample.stockName;
		  xepIter.set(newSample);
//		  size+=Agent.getObjectSize(newSample);
		  cnt++;
		  //System.out.println(newSample.stockName + "\t" + newSample.purchasePrice + "\t" + newSample.purchaseDate);
		}
		System.out.println("Total Amount of transactions read is "+cnt);
		System.out.println("Total Size of transactions read is "+size);
		Long totalTime = System.currentTimeMillis() - startTime;
		xepQuery.close();
		return totalTime;
	}


	// Helper method: Get connection details from config file
	public HashMap<String, String> getConfig(String filename) throws FileNotFoundException, IOException{
        // Initial empty map to store connection details
        HashMap<String, String> map = new HashMap<String, String>();

        map.put("ip","127.0.0.1");
        map.put("port","51773");
        map.put("namespace", "user");
        map.put("username", "superuser");
        map.put("password", "SYS");


        return map;
    }
}
