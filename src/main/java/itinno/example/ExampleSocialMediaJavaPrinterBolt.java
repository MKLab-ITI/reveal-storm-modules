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
//	Created Date :	2014/02/09
//	Created for Project:	REVEAL
//
/////////////////////////////////////////////////////////////////////////
//
// Dependencies: None
//
/////////////////////////////////////////////////////////////////////////

package itinno.example;

import java.util.Map;

// Import core Storm classes
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


/**
 * Example Java Printer Bolt. In this case it will be a simply console "Printer" worker.
 *
 * API (for BaseRichBolt): http://nathanmarz.github.io/storm/doc-0.8.1/index.html (search for "BaseRichBolt") 
 */
public class ExampleSocialMediaJavaPrinterBolt extends BaseRichBolt {
	private OutputCollector collector;
	
	
	/**
	 * Prepare method is similar the "Open" method for Spouts and is called when a worker is about to be put to work.
	 * This method also initialise the main example Storm Java bolt.
	 * 
	 * @param stormConf  map of the storm configuration (passed within Storm topology itself, not be a user)
	 * @param context    context (e.g. similar to description) of the topology (passed within Storm topology itself, not be a user)
	 * @param collector  output collector of the Storm (which is responsible to emiting new tuples, passed within Storm topology itself, not be a user)
	 */
	@Override
	public void prepare( Map stormConf, TopologyContext context, OutputCollector collector ) {
		this.collector = collector;
	}

	
	/**
	 * Execute received Tuple. In this case we will simply print received output.
	 *   
	 * @param input  standard Storm tuple input object (passed within Storm topology itself, not be a user)
	 */
	@Override
	public void execute( Tuple input ) {
		// Print received message
		System.out.println( "Received message: " + input.getValue(0) );
		
		// Acknowledge the collector that we actually received the input
		collector.ack( input );
	}


	/**
	 * Declare output field name (in this case it is not used because printer bolt will not emit any messages to other bolts)
	 * 
	 * @param declarer  standard Storm output fields declarer
	 */
	@Override
	public void declareOutputFields( OutputFieldsDeclarer declarer ) {
		// We will not be declaring and sending any output from this Bolt
	}
}
