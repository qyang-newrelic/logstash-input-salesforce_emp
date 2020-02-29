package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Input;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.lang.Thread;
import com.salesforce.emp.connector.BayeuxParameters;
import com.salesforce.emp.connector.EmpConnector;
import com.salesforce.emp.connector.LoginHelper;
import com.salesforce.emp.connector.TopicSubscription;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.eclipse.jetty.util.ajax.JSON;
import static org.cometd.bayeux.Channel.*;
import com.salesforce.emp.connector.example.BearerTokenProvider;


// class name must match plugin name
@LogstashPlugin(name="salesforce_emp")
public class SalesforceEMP implements Input {

		private final static Logger logger = LogManager.getLogger(SalesforceEMP.class);

    public static final PluginConfigSpec<String> SFDC_USERNAME_CONFIG =
            PluginConfigSpec.stringSetting("username", "test@example.com");

    public static final PluginConfigSpec<String> SFDC_PASSWORD_CONFIG =
            PluginConfigSpec.stringSetting("password", "password");
    
    public static final PluginConfigSpec<String> SFDC_HOST_CONFIG =
            PluginConfigSpec.stringSetting("host", "login.salesforce.com");

   public static final PluginConfigSpec<String> SFDC_EVENT_CONFIG =
            PluginConfigSpec.stringSetting("eventname", "test");

    private String id;
    private String username;
    private String password;
    private String host;
    private String eventname;


    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean stopped;

    // all plugins must provide a constructor that accepts id, Configuration, and Context
    public SalesforceEMP(String id, Configuration config, Context context) {
        // constructors should validate configuration options
				logger.info("init");

        this.id = id;
				if (config == null ) {
					logger.error("no config");
				} else {
       	 username = config.get(SFDC_USERNAME_CONFIG);
       	 password = config.get(SFDC_PASSWORD_CONFIG);
       	 host = config.get(SFDC_HOST_CONFIG);
       	 eventname = config.get(SFDC_EVENT_CONFIG);
				}
				logger.info("host:" + host);
				logger.info("eventname:" + eventname);
    }

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {

        // The start method should push Map<String, Object> instances to the supplied QueueWriter
        // instance. Those will be converted to Event instances later in the Logstash event
        // processing pipeline.
        //
        // Inputs that operate on unbounded streams of data or that poll indefinitely for new
        // events should loop indefinitely until they receive a stop request. Inputs that produce
        // a finite sequence of events should loop until that sequence is exhausted or until they
        // receive a stop request, whichever comes first.


        Consumer<Map<String, Object>> sfdcConsumer = event -> {
					logger.info(String.format("Received from : %s", eventname));
					Map<String,Object> msg = new HashMap();
					msg.put("nr_channel",eventname);
					msg.put("service_name","Salesforce Platform Event");
					msg.put("nr_source",host);
					msg.put("message", event.get((Object) "payload"));
    			consumer.accept(msg);
				};

        TopicSubscription subscription; 

        BearerTokenProvider tokenProvider = new BearerTokenProvider(() -> {
            try {
                return LoginHelper.login(new URL(host), username, password);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        try {
        	BayeuxParameters params = tokenProvider.login();
        	EmpConnector connector = new EmpConnector(params);
        	//LoggingListener loggingListener = new LoggingListener(true, true);
		
        	connector.setBearerTokenProvider(tokenProvider);

        	connector.start().get(5, TimeUnit.SECONDS);

          long replayFrom = EmpConnector.REPLAY_FROM_EARLIEST;

       
          subscription = connector.subscribe(eventname, replayFrom, sfdcConsumer).get(5, TimeUnit.SECONDS);
        	logger.info(String.format("Subscribed: %s", subscription));
        } catch (ExecutionException e) {
            logger.error(e.getCause().toString());
            //System.exit(1);
            //throw e.getCause();
        } catch (TimeoutException e) {
            logger.error("Timed out subscribing");
            logger.error(e.getCause().toString());
            //System.exit(1);
            //throw e.getCause();
        } catch (Exception e) {
	   				logger.error("Error subscribing");
            logger.error(e.getCause().toString());
            stopped = true;
            done.countDown();
				}


        //int eventCount = 0;
        try {
            while (!stopped) {
                //eventCount++;
								Thread.sleep(10000);
            }
        } catch (Exception e) {
            logger.error(e.getCause().toString());
				}
				finally {
            stopped = true;
            done.countDown();
        }
    }

    @Override
    public void stop() {
        stopped = true; // set flag to request cooperative stop of input
    }

    @Override
    public void awaitStop() throws InterruptedException {
        done.await(); // blocks until input has stopped
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        return Arrays.asList(SFDC_USERNAME_CONFIG, SFDC_PASSWORD_CONFIG,SFDC_HOST_CONFIG,SFDC_EVENT_CONFIG);
    }

    @Override
    public String getId() {
        return this.id;
    }
}
