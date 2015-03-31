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
//	Created Date :	2014/03/28
//	Created for Project:	REVEAL
//
/////////////////////////////////////////////////////////////////////////
//
// Dependencies: None
//
/////////////////////////////////////////////////////////////////////////

package itinno.common;

// Import main slf4j logger factory
import org.slf4j.LoggerFactory;

// Import logback core logging parts
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;



public class StormLoggingHelper {
	
	/**
	 * Create logger based on specified arguments (logger name, log file appender name and log level)
	 * 
	 * @param strLoggerName  name of the logging class
	 * @param strLogFileName path (or name) to the log file location
	 * @param logLevel       level of the logging
	 * @return Logger        return initialised ch.qos.logback.classic.Logger object
	 * @throws Exception     raise exception if logger name, log file path (or name), log pattern are empty, as well as in the logLevel is not instance of ch.qos.logback.classic.Logger
	 */
	public Logger createLogger( String strLoggerName, String strLogFileName, String strLogPattern, Level logLevel ) throws Exception {
		
		try {
			// First of all check if the length of the log file name is more than 0
			if ( strLoggerName.isEmpty() ) {
				throw new Exception( "Length of the logger name can not be 0." );
			}
		
			// Then check if the length of the log file name is more than 0
			if ( strLogFileName.isEmpty() ) {
				throw new Exception( "Length of the log file name can not be 0." );
			}
		
			// Check if the log pattern is more than 0
			if ( strLogPattern.isEmpty() ) {
				throw new Exception( "Length of the logging pattern can not be 0." );
			}
			
			// Finally check of the specified log level is an instance of the ch.qos.logback.classic.Level
			if ( !(logLevel instanceof Level) ) {
				throw new Exception( "Invalid type of logging level was specified. Logging level type should be instance of ch.qos.logback.classic.Level." );
			}
			
			// Initialise logger context and logger pattern encoder
			LoggerContext loggerContext = ( LoggerContext ) LoggerFactory.getILoggerFactory();
			PatternLayoutEncoder patternLayoutEncoder = new PatternLayoutEncoder();
			
			// Set logging pattern (UTF-8 log)
			// logging patterns defined at http://logback.qos.ch/manual/layouts.html
			patternLayoutEncoder.setPattern( strLogPattern );
			patternLayoutEncoder.setContext( loggerContext );
			patternLayoutEncoder.setCharset( java.nio.charset.Charset.forName("UTF-8") );
			patternLayoutEncoder.start();
			
			// Setup file appender
			FileAppender<ILoggingEvent> fileAppender = new FileAppender<ILoggingEvent>();
			fileAppender.setFile( strLogFileName );
			fileAppender.setEncoder( patternLayoutEncoder );
			fileAppender.setContext( loggerContext );
			fileAppender.start();
			
			// Finally setup the logger itself
			Logger logger = ( Logger ) LoggerFactory.getLogger( strLoggerName );
			logger.addAppender( fileAppender );
			logger.setLevel( logLevel );
			
			// We are not interested in root logging here
			logger.setAdditive( false ); 
			
			return logger;
		
		// Catch and reissue any possible exception
		} catch ( Exception e ) {
			throw new Exception( e.getMessage() );
		}
	}
}