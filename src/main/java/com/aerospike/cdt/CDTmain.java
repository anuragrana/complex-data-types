package com.aerospike.cdt;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.task.RegisterTask;

public class CDTmain {
	private static final String RECORD_WITH_MAP_MAP_PETER = "record-with-map-map-peter";
	private static final String RECORD_WITH_MAP_PETER = "record-with-map-peter";
	private static final String RECORD_WITH_MAP_LIST_PETER = "record-with-map-list-peter";
	private static final String RECORD_WITH_LIST_MAP_PETER = "record-with-list-map-peter";
	private static final String RECORD_WITH_LIST_LIST_PETER = "record-with-list-list-peter";
	private static final String RECORD_WITH_LIST_PETER = "record-with-list-peter";
	private static Logger log = Logger.getLogger(CDTmain.class);
	AerospikeClient client;
	private String seedHost;
	private int port;
	private String namespace;
	private String set;
	public static void main(String[] args) throws ParseException {
		Options options = new Options();
		options.addOption("h", "host", true, "Server hostname (default: localhost)");
		options.addOption("p", "port", true, "Server port (default: 3000)");
		options.addOption("n", "namespace", true, "Namespace (default: test)");
		options.addOption("s", "set", true, "Set to delete (default: test)");
		options.addOption("u", "usage", false, "Print usage.");

		CommandLineParser parser = new PosixParser();
		CommandLine cl = parser.parse(options, args, false);

		if (args.length == 0 || cl.hasOption("u")) {
			logUsage(options);
			return;
		}

		String host = cl.getOptionValue("h", "127.0.0.1");
		String portString = cl.getOptionValue("p", "3000");
		int port = Integer.parseInt(portString);
		String set = cl.getOptionValue("s", "test");
		String namespace = cl.getOptionValue("n","test");

		log.info("Host: " + host);
		log.info("Port: " + port);
		log.info("Name space: " + namespace);
		log.info("Set: " + set);

		CDTmain cdt = null;
		try {
			cdt = new CDTmain(host, port, namespace, set);
		} catch (AerospikeException e) {
			log.error("Could not connect to Aerospike");
			log.debug("Detailed error:", e);
			return;
		}
		try {
			cdt.registerUDF();
		} catch (AerospikeException e) {
			log.error("Could not register UDF");
			log.error(ResultCode.getResultString(e.getResultCode()));
			log.debug("Detailed error:", e);
			return;
		}
		cdt.map();
		cdt.mapMap();
		cdt.list();
		cdt.listList();
		cdt.mapList();
		cdt.listMap();

	}
	private void listMap() {
		log.info("*** List-Map");
		try {
			WritePolicy writePolicy = new WritePolicy();
			Policy policy = new Policy();
			/*
			 * build a list of maps to store in a bin
			 */
			List<Map<String, Object>> flightList = new ArrayList<Map<String, Object>>();
			flightList.add(new HashMap<String, Object>(){{
				put("airline","united");
				put("time", 3);
				put("number", 2351);
			}});
			flightList.add(new HashMap<String, Object>(){{
				put("airline","singapore");
				put("time", 3);
				put("number", 8224);
			}});
			flightList.add(new HashMap<String, Object>(){{
				put("airline","qantas");
				put("time", 3);
				put("number",1255);
			}});
			flightList.add(new HashMap<String, Object>(){{
				put("airline","virgin");
				put("time", 3);
				put("number",2351);
			}});
			flightList.add(new HashMap<String, Object>(){{
				put("airline","american");
				put("time", 3);
				put("number",11943);
			}});
			flightList.add(new HashMap<String, Object>(){{
				put("airline","emirates");
				put("time", 3);
				put("number",2351);
			}});
			/*
			 * write the record
			 */
			this.client.put(writePolicy, new Key(namespace, set, Value.get(RECORD_WITH_LIST_MAP_PETER)), 
					new Bin("name", Value.get("peter")),
					new Bin("number", Value.get(1234567890)),
					new Bin("flight-map", Value.getAsList(flightList))
					);
			/*
			 * read the record and print it
			 */
			Record record = client.get(policy, new Key(namespace, set, Value.get(RECORD_WITH_LIST_MAP_PETER)));
			log.info("Record: " + record);
			/*
			 * invoke the UDF to get an element from a document
			 */
			List<? extends Object> path = Arrays.asList(3, "airline");
			log.info("Path: " + path);
			Object result = client.execute(policy, 
					new Key(namespace, set, Value.get(RECORD_WITH_LIST_MAP_PETER)), 
					"document", "get", Value.get("flight-map"), Value.getAsList(path));
			log.info("UDF element: " + result);
			/*
			 * delete the record
			 *
			 */
			client.delete(writePolicy, new Key(namespace, set, Value.get(RECORD_WITH_LIST_MAP_PETER)));
		} catch (AerospikeException e){
			log.error(ResultCode.getResultString(e.getResultCode()));
			log.debug("Detailed error:", e);
		}

		
	}
	private void mapList() {
		try {
			log.info("*** Map-List");
			WritePolicy writePolicy = new WritePolicy();
			Policy policy = new Policy();
			/*
			 * build a map of maps to store in a bin
			 */
			Map<String, List<String>> flightMap = new HashMap<String, List<String>>();
			flightMap.put("DFW-SFO", Arrays.asList("cats", "dogs", "mice", "snakes"));
			flightMap.put("SFO-NRT", Arrays.asList("cats", "dogs", "mice", "snakes", "monkets"));
			flightMap.put("NRT-ICN", Arrays.asList("cats", "dogs", "mice", "snakes", "elephants"));
			flightMap.put("ICN-SYD", Arrays.asList("snakes", "cats", "dogs", "mice"));
			flightMap.put("SYD-SFO", Arrays.asList("mice", "snakes", "cats", "dogs"));
			flightMap.put("SFO-DFW", Arrays.asList("dogs", "mice", "snakes", "cats"));
			/*
			 * write the record
			 */
			this.client.put(writePolicy, new Key(namespace, set, Value.get(RECORD_WITH_MAP_LIST_PETER)), 
					new Bin("name", Value.get("peter")),
					new Bin("number", Value.get(1234567890)),
					new Bin("flight-map", Value.getAsMap(flightMap))
					);
			/*
			 * read the record and print it
			 */
			Record record = client.get(policy, new Key(namespace, set, Value.get(RECORD_WITH_MAP_LIST_PETER)));
			log.info("Record: " + record);
			/*
			 * invoke the UDF to get an element from a document
			 */
			List<? extends Object> path = Arrays.asList("NRT-ICN", 5);
			log.info("Path: " + path);
			Object result = client.execute(policy, 
					new Key(namespace, set, Value.get(RECORD_WITH_MAP_LIST_PETER)), 
					"document", "get", Value.get("flight-map"), Value.getAsList(path));
			log.info("UDF element: " + result);
			/*
			 * delete the record
			 *
			 */
			client.delete(writePolicy, new Key(namespace, set, Value.get(RECORD_WITH_MAP_LIST_PETER)));
		} catch (AerospikeException e){
			log.error(ResultCode.getResultString(e.getResultCode()));
			log.debug("Detailed error:", e);
		}

	}
	private void listList() {
		log.info("*** List-List");
		try {
			WritePolicy writePolicy = new WritePolicy();
			Policy policy = new Policy();
			/*
			 * build a list of lists to store in a bin
			 */
			List<List<? extends Object>> flightList = new ArrayList<List<? extends Object>>();
			flightList.add(Arrays.asList("united", 3, 2351));
			flightList.add(Arrays.asList("singapore", 3, 8224));
			flightList.add(Arrays.asList("qantas", 3, 1255));
			flightList.add(Arrays.asList("virgin", 3, 2351));
			flightList.add(Arrays.asList("american", 3, 11943));
			flightList.add(Arrays.asList("emirates", 3, 2351));
			/*
			 * write the record
			 */
			this.client.put(writePolicy, new Key(namespace, set, Value.get(RECORD_WITH_LIST_LIST_PETER)), 
					new Bin("name", Value.get("peter")),
					new Bin("number", Value.get(1234567890)),
					new Bin("flight-map", Value.getAsList(flightList))
					);
			/*
			 * read the record and print it
			 */
			Record record = client.get(policy, new Key(namespace, set, Value.get(RECORD_WITH_LIST_LIST_PETER)));
			log.info("Record: " + record);
			/*
			 * invoke the UDF to get an element from a document
			 */
			List<? extends Object> path = Arrays.asList(3, 1);
			log.info("Path: " + path);
			Object result = client.execute(policy, 
					new Key(namespace, set, Value.get(RECORD_WITH_LIST_LIST_PETER)), 
					"document", "get", Value.get("flight-map"), Value.getAsList(path));
			log.info("UDF element: " + result);
			/*
			 * delete the record
			 *
			 */
			client.delete(writePolicy, new Key(namespace, set, Value.get(RECORD_WITH_LIST_MAP_PETER)));
		} catch (AerospikeException e){
			log.error(ResultCode.getResultString(e.getResultCode()));
			log.debug("Detailed error:", e);
		}

	}
	private void list() {
		log.info("*** List");
		// TODO Auto-generated method stub
		
	}
	private void registerUDF() throws AerospikeException {
		/*
		 * register the UDF that will mutate the Map
		 * on the server side
		 */

		File udfFile = new File("udf/document.lua");
		RegisterTask task = this.client.register(null, 
				udfFile.getPath(), 
				udfFile.getName(), 
				Language.LUA); 
		task.waitTillComplete();

	}
	private void map() {
		log.info("*** Map");
		try {
			WritePolicy writePolicy = new WritePolicy();
			Policy policy = new Policy();
			/*
			 * build a map to store in a bin
			 */
			Map<String, Integer> flightMap = new HashMap<String, Integer>();
			flightMap.put("DFW-SFO", 2351);
			flightMap.put("SFO-NRT", 8224);
			flightMap.put("NRT-ICN", 1255);
			flightMap.put("ICN-SYD", 2351);
			flightMap.put("SYD-SFO", 11943);
			flightMap.put("SFO-DFW", 2351);
			/*
			 * write the record
			 */
			this.client.put(writePolicy, new Key(namespace, set, Value.get(RECORD_WITH_MAP_PETER)), 
					new Bin("name", Value.get("peter")),
					new Bin("number", Value.get(1234567890)),
					new Bin("flight-map", Value.getAsMap(flightMap))
					);
			/*
			 * read the record and print it
			 */
			Record record = client.get(policy, new Key(namespace, set, Value.get(RECORD_WITH_MAP_PETER)));
			log.info("Record: " + record);
			/*
			 * invoke the UDF to get an element from a document
			 */
			Object result = client.execute(policy, 
					new Key(namespace, set, Value.get(RECORD_WITH_MAP_PETER)), 
					"document", "get", Value.get("flight-map"), Value.get("SYD-SFO"));
			log.info("UDF element: " + result);
			/*
			 * delete the record
			 *
			 */
			client.delete(writePolicy, new Key(namespace, set, Value.get(RECORD_WITH_MAP_PETER)));
		} catch (AerospikeException e){
			log.error(ResultCode.getResultString(e.getResultCode()));
			log.debug("Detailed error:", e);
		}

	}
	private void mapMap() {
		log.info("*** Map-Map");
		try {
			WritePolicy writePolicy = new WritePolicy();
			Policy policy = new Policy();
			/*
			 * build a map of maps to store in a bin
			 */
			Map<String, Map<String, Object>> flightMap = new HashMap<String, Map<String, Object>>();
			flightMap.put("DFW-SFO", new HashMap<String, Object>(){{
				put("airline","united");
				put("time", 3);
				put("number", 2351);
			}});
			flightMap.put("SFO-NRT", new HashMap<String, Object>(){{
				put("airline","singapore");
				put("time", 3);
				put("number", 8224);
			}});
			flightMap.put("NRT-ICN", new HashMap<String, Object>(){{
				put("airline","qantas");
				put("time", 3);
				put("number",1255);
			}});
			flightMap.put("ICN-SYD", new HashMap<String, Object>(){{
				put("airline","virgin");
				put("time", 3);
				put("number",2351);
			}});
			flightMap.put("SYD-SFO", new HashMap<String, Object>(){{
				put("airline","american");
				put("time", 3);
				put("number",11943);
			}});
			flightMap.put("SFO-DFW", new HashMap<String, Object>(){{
				put("airline","emirates");
				put("time", 3);
				put("number",2351);
			}});
			/*
			 * write the record
			 */
			this.client.put(writePolicy, new Key(namespace, set, Value.get(RECORD_WITH_MAP_MAP_PETER)), 
					new Bin("name", Value.get("peter")),
					new Bin("number", Value.get(1234567890)),
					new Bin("flight-map", Value.getAsMap(flightMap))
					);
			/*
			 * read the record and print it
			 */
			Record record = client.get(policy, new Key(namespace, set, Value.get(RECORD_WITH_MAP_MAP_PETER)));
			log.info("Record: " + record);
			/*
			 * invoke the UDF to get an element from a document
			 */
			List<String> path = Arrays.asList("ICN-SYD", "airline");
			log.info("Path: " + path);
			Object result = client.execute(policy, 
					new Key(namespace, set, Value.get(RECORD_WITH_MAP_MAP_PETER)), 
					"document", "get", Value.get("flight-map"), Value.getAsList(path));
			log.info("UDF element: " + result);
			/*
			 * delete the record
			 *
			 */
			client.delete(writePolicy, new Key(namespace, set, Value.get(RECORD_WITH_MAP_MAP_PETER)));
		} catch (AerospikeException e){
			log.error(ResultCode.getResultString(e.getResultCode()));
			log.debug("Detailed error:", e);
		}

	}
	private static void logUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = CDTmain.class.getName() + " [<options>]";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		log.info(sw.toString());
	}

	public CDTmain(String seedHost, int port, String namespace, String set) throws AerospikeException{
		this.seedHost = seedHost;
		this.port = port;this.client = new AerospikeClient(this.seedHost, this.port);
		this.namespace = namespace;
		this.set = set;

	}

}
