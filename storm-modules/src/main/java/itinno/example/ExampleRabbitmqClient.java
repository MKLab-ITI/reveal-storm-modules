/////////////////////////////////////////////////////////////////////////
//
// \xa9 University of Southampton IT Innovation, 2014
//
// Copyright in this software belongs to IT Innovation Centre of
// Gamma House, Enterprise Road, Southampton SO16 7NS, UK.
//
// This software may not be used, sold, licensed, transferred, copied
// or reproduced in whole or in part in any manner or form or in or
// on any media by any person other than in accordance with the terms
// of the Licence Agreement supplied with the software, or otherwise
// without the prior written consent of the copyright owners.
//
// This software is distributed WITHOUT ANY WARRANTY, without even the
// implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
// PURPOSE, except where stated in the Licence Agreement supplied with
// the software.
//
//	Created By :	Vadim Krivcov
//	Created Date :	2014/03/31
//	Created for Project:	REVEAL
//
/////////////////////////////////////////////////////////////////////////
//
// Dependencies: None
//
/////////////////////////////////////////////////////////////////////////

package itinno.example;

// General helper imports

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;

// Import slf4j logging 
import gr.iti.mklab.framework.client.search.visual.VisualIndexHandler;
import gr.iti.mklab.visual.VisualIndexer;
import gr.iti.mklab.visual.aggregation.AbstractFeatureAggregator;
import gr.iti.mklab.visual.aggregation.VladAggregatorMultipleVocabularies;
import gr.iti.mklab.visual.dimreduction.PCA;
import gr.iti.mklab.visual.extraction.AbstractFeatureExtractor;
import gr.iti.mklab.visual.extraction.SURFExtractor;
import gr.iti.mklab.visual.vectorization.ImageVectorization;
import org.slf4j.LoggerFactory;

// Import RabbitMQ connection library files
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

// import logback logging classes
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Example java RabbitMQ publisher for use in sending JSON objects to Storm spout
 */
public class ExampleRabbitmqClient {

    public static void main(String[] args) {
        Logger logger = null;
        String patternLayout = "%5p %d{yyyy-MM-dd HH:mm:ss,sss} %file %t %L: %m%n";

        try {
            // Initialise logger context and logger pattern encoder
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            PatternLayoutEncoder patternLayoutEncoder = new PatternLayoutEncoder();

            // Set logging pattern (UTF-8 log)
            // logging patterns defined at http://logback.qos.ch/manual/layouts.html
            patternLayoutEncoder.setPattern(patternLayout);
            patternLayoutEncoder.setContext(loggerContext);
            patternLayoutEncoder.setCharset(Charset.forName("UTF-8"));
            patternLayoutEncoder.start();

            // log to console
            ConsoleAppender<ILoggingEvent> appender = new ConsoleAppender<ILoggingEvent>();
            appender.setContext(loggerContext);
            appender.setEncoder(patternLayoutEncoder);
            appender.start();

            // Finally setup the logger itself
            logger = (Logger) LoggerFactory.getLogger(ExampleRabbitmqClient.class.getName());
            logger.addAppender(appender);
            logger.setLevel(Level.DEBUG);
            logger.setAdditive(false);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        /*try {
            VisualIndexer.init("/home/kandreadou/webservice/learning_files/","127.0.0.1",LoggerFactory.getLogger(ExampleRabbitmqClient.class));
            VisualIndexer v = new VisualIndexer("simtest");
            v.index("http://resources0.news.com.au/images/2012/08/16/1226451/587056-120816-obama.jpg","obama1");
        } catch (Exception ex) {
            System.out.println(ex);
        }*/

        logger.info("rabitmq client started");

        // send a UTF-8 encoded JSON tweet to the RabbitMQ (for stormspout to pick up and send to bolt)
        try {
            // check args
            /*if ( args.length < 1 ) {
				logger.error( "Usage: example_rabbitmq_client <JSON_filename>" );
				System.exit( 1 );
			}
            logger.info( "JSON file = " + args[0] );*/
            String strJSONFilePath = "/home/kandreadou/mklab/example-tweet-utf8.json";


            // check if the path to JSON file exists
            File fileJSON = new File(strJSONFilePath);

            System.out.println(fileJSON.getAbsolutePath());

            if (fileJSON.exists() == false) {
                logger.info("JSON file does not exist : " + strJSONFilePath);
                logger.info("Usage: example_rabbitmq_client <JSON_filename>");
                System.exit(1);
            }


            // read UTF-8 JSON text from file
            logger.info("ExampleRabbitmqClient started");

            List<String> lines = Files.readAllLines(Paths.get(strJSONFilePath), Charset.forName("UTF-8"));
            String strJSON = "";
            for (String line : lines) {
                if (line.length() > 0) {
                    strJSON = strJSON + "\n";
                }
                strJSON = strJSON + line;
            }
            logger.info("JSON test message = " + strJSON);

            // connect to rabbitmq broker
            // first of all create connection factory
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri("amqp://guest:guest@localhost:5672/%2F");
            long timestampSinceEpoch = System.currentTimeMillis() / 1000;

            // initialise connection and define channel
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            // initialise amqp basic peoperties object
            BasicProperties.Builder basicProperties = new BasicProperties.Builder();
            basicProperties.build();
            basicProperties.timestamp(new Date(timestampSinceEpoch)).build();
            basicProperties.contentType("text/json").build();
            basicProperties.deliveryMode(1).build();

            // publish message
            /* Exchange name should be {assessment_id}+ "_exchange" */
            channel.basicPublish("test_exchange", "test-routing", basicProperties.build(), strJSON.getBytes("UTF-8"));

            // close connection and channel
            channel.close();
            connection.close();

            logger.info("ExampleRabbitmqClient finished");

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            System.exit(1);
        }
    }
}
