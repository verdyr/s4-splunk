/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.s4.adapter.splunk;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.net.ServerSocket;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.HashMap;

import org.apache.s4.base.Event;
import org.apache.s4.core.adapter.AdapterApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.splunk.*;
//import com.splunk.


public class SplunkInputAdapter extends AdapterApp {

    int countReceivedMsgs = 0;
    private static Logger logger = LoggerFactory.getLogger(SplunkInputAdapter.class);

        public SplunkInputAdapter() {
    }

    private LinkedBlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>();

    protected ServerSocket serverSocket;

    private Thread t;

    @Override
    protected void onClose() {
    }

    @Override
    protected void onInit() {
        super.onInit();
        t = new Thread(new Dequeuer());
    }

    public void connectAndRead() throws Exception {

        // Building query
        String exportQuery = "search index=SERVICE_NAME sourcetype=SOURCE_TYPE mode=SOURCE_MODE";

        ServiceArgs cb = new ServiceArgs();
	JobExportArgs exportArgs = new JobExportArgs();

	// Output mode, xml, json, raw ...
        exportArgs.setOutputMode(JobExportArgs.OutputMode.CSV);

	// Search parameters, Export vs Realtime
        exportArgs.setEarliestTime("-1m");
	exportArgs.setLatestTime("now");
	exportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL);
/*
        exportArgs.setEarliestTime("rt-30s");
        exportArgs.setLatestTime("rt");
	exportArgs.setSearchMode(JobExportArgs.SearchMode.REALTIME);
*/

	Properties splunkProperties = new Properties();

        File splunkPropsFile = new File(System.getProperty("user.home") + "/splunk.properties");
//        File splunkPropsFile = new File("/home/ec2-user/splunk.properties");
        if (!splunkPropsFile.exists()) {
            logger.error(
                    "Cannot find splunk.properties file in this location :[{}]. Make sure it is available at this place and includes user/password credentials",
                    splunkPropsFile.getAbsolutePath());
            return;
        }
        splunkProperties.load(new FileInputStream(splunkPropsFile));


//        System.setProperty("https.proxyHost","proxy.address.com"); // SB, hardcoded proxy support
//        System.setProperty("https.proxyPort","8080"); // SB, hardcoded proxy support

//        cb.setDebugEnabled(Boolean.valueOf(splunkProperties.getProperty("debug")))
//                .setUser(splunkProperties.getProperty("user")).setPassword(splunkProperties.getProperty("password"));

	cb.setUsername(splunkProperties.getProperty("user"));
	cb.setPassword(splunkProperties.getProperty("password"));
	cb.setHost(splunkProperties.getProperty("host"));
	cb.setPort(Integer.valueOf(splunkProperties.getProperty("port")));

        Service splunkService = new Service(cb);
        splunkService.login();

	// Getting max results from the Splunk server configuration
	Entity restApi = splunkService.getConfs().get("limits").get("restapi");
	int maxResults = Integer.parseInt((String) restApi.get("maxresultrows"));

	while (true) {

	        InputStream exportStream = splunkService.export(exportQuery, exportArgs);
		String line = null;
		BufferedReader reader = new BufferedReader(new InputStreamReader(exportStream, "UTF-8"));
		while ( (line = reader.readLine() ) != null) {
			logger.info("Message: [{}]", line);
	                messageQueue.add(line);
		}
	}

// THIS IS FOR TESTING ONLY
/*
	ResultsReaderXml resultsReader = new ResultsReaderXml(exportSearch); 
	logger.info("Message received from ResultsReaderXml: [{}]", resultsReader.toString());
*/
    }

    @Override
    protected void onStart() {
        try {
            t.start();
            connectAndRead();
	    logger.info("Connect and Read loaded");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    class Dequeuer implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {

		    countReceivedMsgs++;
		    logger.info("Total messages received: [{}]", countReceivedMsgs);
                    String results = messageQueue.take();
		    
		    Event routingEvent = new Event();

		// Event 
		    routingEvent.put("routingText", String.class, results);
		    logger.info("Message received: [{}]", results);
		    getRemoteStream().put(routingEvent);

                } catch (Exception e) {

                }
            }

        }
    }
}
