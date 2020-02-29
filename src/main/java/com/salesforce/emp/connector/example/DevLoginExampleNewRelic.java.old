/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.TXT file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.emp.connector.example;

import com.salesforce.emp.connector.BayeuxParameters;
import com.salesforce.emp.connector.EmpConnector;
import com.salesforce.emp.connector.LoginHelper;
import com.salesforce.emp.connector.TopicSubscription;

import java.net.URL;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.eclipse.jetty.util.ajax.JSON;

import static org.cometd.bayeux.Channel.*;
import com.salesforce.emp.connector.example.nrLogClient;


/**
 * An example of using the EMP connector
 *
 * @author hal.hildebrand
 * @since API v37.0
 */
public class DevLoginExampleNewRelic {

    public static void main(String[] argv) throws Throwable {
        if (argv.length < 5 || argv.length > 6) {
            System.err.println("Usage: DevLoginExample url username password topic nr_license_key [replayFrom]");
            System.exit(1);
        } 
				// New Relic 
				String nrLicenseKey = argv[4];
				nrLogClient logClient = new nrLogClient();
				logClient.setLicenseKey(nrLicenseKey);

        Consumer<Map<String, Object>> consumer = event -> {
					System.out.println(String.format("Received:\n%s", JSON.toString(event)));
					Map<String,String> atts = new HashMap();
					atts.put("channel",argv[3]);
					atts.put("service_name","Salesforce Platform Event");
					atts.put("source",argv[0]);
					logClient.sendWithFields(JSON.toString(event),atts);
					};

        BearerTokenProvider tokenProvider = new BearerTokenProvider(() -> {
            try {
                return LoginHelper.login(new URL(argv[0]), argv[1], argv[2]);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        BayeuxParameters params = tokenProvider.login();
        
        EmpConnector connector = new EmpConnector(params);
        LoggingListener loggingListener = new LoggingListener(true, true);
				/* 
        connector.addListener(META_HANDSHAKE, loggingListener)
                .addListener(META_CONNECT, loggingListener)
                .addListener(META_DISCONNECT, loggingListener);
        connector.addListener(META_HANDSHAKE, loggingListener)
                .addListener(META_CONNECT, loggingListener)
                .addListener(META_DISCONNECT, loggingListener)
                .addListener(META_SUBSCRIBE, loggingListener)
                .addListener(META_UNSUBSCRIBE, loggingListener);
				*/

        connector.setBearerTokenProvider(tokenProvider);

        connector.start().get(5, TimeUnit.SECONDS);

        long replayFrom = EmpConnector.REPLAY_FROM_EARLIEST;
        if (argv.length == 6) {
            replayFrom = Long.parseLong(argv[4]);
        }

        TopicSubscription subscription;
        try {
            subscription = connector.subscribe(argv[3], replayFrom, consumer).get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            System.err.println(e.getCause().toString());
            System.exit(1);
            throw e.getCause();
        } catch (TimeoutException e) {
            System.err.println("Timed out subscribing");
            System.exit(1);
            throw e.getCause();
        }

        System.out.println(String.format("Subscribed: %s", subscription));
    }
}
