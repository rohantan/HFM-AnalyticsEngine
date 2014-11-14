package AnalyticsEngine.AE;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import analytics.Analytics;
import analytics.Analytics.AnRecord;
import analytics.Analytics.Interface;
import analytics.Analytics.System;

import com.googlecode.protobuf.format.JsonFormat;

public class JavaQueueReceiver extends Receiver<AnRecord> {
	public static final Logger logger = LoggerFactory.getLogger(JavaQueueReceiver.class);
	
	public static HashMap<String, Analytics.System> deviceInfo = new HashMap<String, Analytics.System>();
	public static HashMap<String, Interface> interfaceInfo = new HashMap<String, Interface>();
	static HashMap<String, ArrayList<Long>> interfaceQueueStats = new HashMap<String, ArrayList<Long>>();
	public static HashMap<String, HashMap<Integer, Long>> interfaceQueueStatsInfo = new HashMap<String, HashMap<Integer, Long>>();
	public static HashMap<String, HashMap<Integer, Double>> interfaceLatencyStatsInfo = new HashMap<String, HashMap<Integer, Double>>();
	public static LinkedHashMap<String, Double> interfaceTopLatencyStatsInfo = new LinkedHashMap<String, Double>();
	
	public static HashMap<String, ArrayList<Long>> interfaceQueueStatsChart1 = new HashMap<String, ArrayList<Long>>();
	public static HashMap<String, ArrayList<Long>> interfaceQueueAvgStatsChart1 = new HashMap<String, ArrayList<Long>>();
	public static HashMap<String, ArrayList<Double>> interfaceLatencyStatsChart1 = new HashMap<String, ArrayList<Double>>();
	
	
	public static void main(String[] args) {
		startServer();
	}
	
	public static void startServer() {
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("JavaQueueReceiver");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));

		// Create a DStream that will connect to hostname:port, like localhost:9999
		JavaReceiverInputDStream<AnRecord> records = jssc.receiverStream(new JavaQueueReceiver("localhost", 50005));
		// Logic For each interface
		// Fetch each record fetch the required attributes.
		JavaDStream<String> statsPerInterface = records.flatMap(
			new FlatMapFunction<AnRecord, String>() {
				public Iterable<String> call(AnRecord x) {
					ArrayList<String> statsPerInterface = new ArrayList<String>();
					for (Interface interface1 : x.getInterfaceList()) {
						ArrayList<Long> queueStats = interfaceQueueStats.get(interface1.getName());
						if(queueStats == null)
							queueStats = new ArrayList<Long>();
						queueStats.add(interface1.getStats().getQueueStats().getQueueDepth());
						interfaceQueueStats.put(interface1.getName(), queueStats);
						for (Long stats : queueStats) {
							statsPerInterface.add(interface1.getName() +"," + stats);
						}
						logger.warn("Current status :" + interface1.getName() + ": " + interface1.getStats().getQueueStats().getQueueDepth());
						
						HashMap<Integer, Long> stats = interfaceQueueStatsInfo.get(interface1.getName());
						HashMap<Integer, Double> latStats = interfaceLatencyStatsInfo.get(interface1.getName());
						if(stats == null) {
							stats = new HashMap<Integer, Long>();
							latStats = new HashMap<Integer, Double>();
						}
						stats.put(1,interface1.getStats().getQueueStats().getQueueDepth());
						latStats.put(1, calculateDelay(interface1.getStats().getQueueStats().getQueueDepth(), interfaceInfo.get(interface1.getName()).getStatus().getLink().getSpeed()));
						interfaceQueueStatsInfo.put(interface1.getName(), stats);
						interfaceLatencyStatsInfo.put(interface1.getName(), latStats);
						
						// Code for displaying chart 1.
						ArrayList<Long> curStats = interfaceQueueStatsChart1.get(interface1.getName());
						if(curStats == null) 
							curStats = new ArrayList<Long>();
						if(curStats.size() == 300)
							curStats.remove(0);
						curStats.add(interface1.getStats().getQueueStats().getQueueDepth());
						interfaceQueueStatsChart1.put(interface1.getName(), curStats);
						
						// Code for displaying chart 1.
						ArrayList<Double> curLatStats = interfaceLatencyStatsChart1.get(interface1.getName());
						if(curLatStats == null) 
							curLatStats = new ArrayList<Double>();
						if(curLatStats.size() == 300)
							curLatStats.remove(0);
						curLatStats.add(calculateDelay(interface1.getStats().getQueueStats().getQueueDepth(), interfaceInfo.get(interface1.getName()).getStatus().getLink().getSpeed()));
						interfaceLatencyStatsChart1.put(interface1.getName(), curLatStats);
					}
					//logger.warn("Current status : " + x.getInterface(0).getName() + " : " + (x.getInterface(0).getStats().getQueueStats().getQueueDepth() / (interfaceInfo.get(x.getInterface(0).getName()).getStatus().getLink().getSpeed() * 1.0)));
					
					return statsPerInterface;
				}
			});

		// Count each interface and queue depth
		JavaPairDStream<String, Long> interfacePairs = statsPerInterface.mapToPair(
			new PairFunction<String, String, Long>() {
				public Tuple2<String, Long> call(String s) throws Exception {
					String[] tuple = s.split(",");
					return new Tuple2<String, Long>(tuple[0], Long.parseLong(tuple[1]));
				}
			});
		
		JavaPairDStream<String, Long> maxStatsPerInterface = interfacePairs.reduceByKey(
			new Function2<Long, Long, Long>() {
				public Long call(Long i1, Long i2) throws Exception {
					if(i1 > i2) 
						return i1;
					else
						return i2;
				}
			});
		
		maxStatsPerInterface.foreachRDD(
			new Function<JavaPairRDD<String, Long>, Void> () {
				public Void call(JavaPairRDD<String, Long> rdd) {
					for (Tuple2<String, Long> t: rdd.collect()) {
			        	//logger.warn("Maximum status :" + t._1() + ": " + (t._2() / (interfaceInfo.get(t._1()).getStatus().getLink().getSpeed() * 1.0)));
						logger.warn("Maximum status :" + t._1() + ": " + t._2());
						HashMap<String, Long> data = new HashMap<String, Long>();
						HashMap<Integer, Long> stats = interfaceQueueStatsInfo.get(t._1());
						HashMap<Integer, Double> latStats = interfaceLatencyStatsInfo.get(t._1());
						if(stats == null) {
							stats = new HashMap<Integer, Long>();
							latStats = new HashMap<Integer, Double>();
						}
						stats.put(2,t._2());
						latStats.put(2, calculateDelay(t._2(),interfaceInfo.get(t._1()).getStatus().getLink().getSpeed()));
						interfaceQueueStatsInfo.put(t._1(), stats);
						interfaceLatencyStatsInfo.put(t._1(), latStats);
			        }
			        return null;
				}
			}
		);

		
		JavaPairDStream<String, Long> minStatsPerInterface = interfacePairs.reduceByKey(
			new Function2<Long, Long, Long>() {
				public Long call(Long i1, Long i2) throws Exception {
					if(i1 < i2) 
						return i1;
					else
						return i2;
				}
			}
		);
		
		//minStatsPerInterface.print();

		minStatsPerInterface.foreachRDD(
			new Function<JavaPairRDD<String, Long>, Void> () {
				public Void call(JavaPairRDD<String, Long> rdd) {
					for (Tuple2<String, Long> t: rdd.collect()) {
						//logger.warn("Minimum status :" + t._1() + ": " + (t._2() / (interfaceInfo.get(t._1()).getStatus().getLink().getSpeed() * 1.0)));
			        	logger.warn("Minimum status :" + t._1() + ": " + t._2());
			        	HashMap<Integer, Long> stats = interfaceQueueStatsInfo.get(t._1());
			        	HashMap<Integer, Double> latStats = interfaceLatencyStatsInfo.get(t._1());
						if(stats == null) {
							stats = new HashMap<Integer, Long>();
							latStats = new HashMap<Integer, Double>();
						}
						stats.put(3, t._2());
						latStats.put(3, calculateDelay(t._2(),interfaceInfo.get(t._1()).getStatus().getLink().getSpeed()));
						interfaceQueueStatsInfo.put(t._1(), stats);
						interfaceLatencyStatsInfo.put(t._1(), latStats);
			        }
			        return null;
				}
			}
		);
		
		JavaPairDStream<String, Long> totalStatsPerInterface = interfacePairs.reduceByKey(
			new Function2<Long, Long, Long>() {
				public Long call(Long i1, Long i2) throws Exception {
					return i1 + i2;
				}
			}
		);
		
		totalStatsPerInterface.foreachRDD(
			new Function<JavaPairRDD<String, Long>, Void> () {
				public Void call(JavaPairRDD<String, Long> rdd) {
			        for (Tuple2<String, Long> t: rdd.collect()) {
			        	//logger.warn("Average status :" + t._1() + ": " + (t._2() / (interfaceQueueStats.get(t._1()).size() * interfaceInfo.get(t._1()).getStatus().getLink().getSpeed() * 1.0)));
			        	logger.warn("Average status :" + t._1() + ": " + (t._2() / (interfaceQueueStats.get(t._1()).size())));
			        	HashMap<Integer, Long> stats = interfaceQueueStatsInfo.get(t._1());
			        	HashMap<Integer, Double> latStats = interfaceLatencyStatsInfo.get(t._1());
						if(stats == null) {
							stats = new HashMap<Integer, Long>();
						}
						stats.put(4, (t._2() / (interfaceQueueStats.get(t._1()).size())));
						latStats.put(4, calculateDelay(t._2(),interfaceInfo.get(t._1()).getStatus().getLink().getSpeed()));
						interfaceQueueStatsInfo.put(t._1(), stats);
						interfaceLatencyStatsInfo.put(t._1(), latStats);
						
						// Code for displaying chart 3.
						ArrayList<Long> avgStats = interfaceQueueAvgStatsChart1.get(t._1());
						if(avgStats == null) 
							avgStats = new ArrayList<Long>();
						if(avgStats.size() == 300)
							avgStats.remove(0);
						avgStats.add((t._2() / (interfaceQueueStats.get(t._1()).size())));
						interfaceQueueAvgStatsChart1.put(t._1(), avgStats);
						
			        }
			        return null;
				}
			}
		);
		
		
		/*JavaPairDStream<String, Long> counts = interfacePairs.reduceByKeyAndWindow(
	      new Function2<Long, Long, Long>() {
	        public Long call(Long i1, Long i2) { return i1 + i2; }
	      },
	      new Function2<Long, Long, Long>() {
	        public Long call(Long i1, Long i2) { return i1 - i2; }
	      },
	      new Duration(60 * 5 * 1000),
	      new Duration(1 * 1000)
	    );*/
		
		JavaPairDStream<Long, String> swappedCounts = maxStatsPerInterface.mapToPair(
			     new PairFunction<Tuple2<String, Long>, Long, String>() {
			       public Tuple2<Long, String> call(Tuple2<String, Long> in) {
			         return in.swap();
			       }
			     }
			   );

			   JavaPairDStream<Long, String> sortedCounts = swappedCounts.transformToPair(
			     new Function<JavaPairRDD<Long, String>, JavaPairRDD<Long, String>>() {
			       public JavaPairRDD<Long, String> call(JavaPairRDD<Long, String> in) throws Exception {
			         return in.sortByKey(false);
			       }
			     });

			   sortedCounts.foreachRDD(
			     new Function<JavaPairRDD<Long, String>, Void> () {
			       public Void call(JavaPairRDD<Long, String> rdd) {
			         String out = "\nTop 10 hashtags:\n";
			         for (Tuple2<Long, String> t: rdd.take(3)) {
			        	 interfaceTopLatencyStatsInfo.put(t._2(), calculateDelay(t._1(),interfaceInfo.get(t._2()).getStatus().getLink().getSpeed()));
			         }
			        
			         return null;
			       }
			     }
			   );
		
		/*JavaPairDStream<String, Long> currentStatsPerInterface = interfacePairs.reduceByKey(
		  new Function2<Long, Long, Long>() {
		    public Long call(Long i1, Long i2) throws Exception {
		    	return i2;
		    }
		  });

		
		
		//avgStatsPerInterface.print();
		currentStatsPerInterface.print();
		*/
		
		jssc.start();
		jssc.awaitTermination();
	}

	static double calculateDelay(Long depth, Long speed) {
		return (depth *  8.0 * 1000000) / (speed * 1.0);
	}
	
	public static String getDevices() {
		JSONObject jo = new JSONObject();
		Iterator<Entry<String, System>> it = deviceInfo.entrySet().iterator();
		ArrayList<String> devices = new ArrayList<String>();
		try {
			while (it.hasNext()) {
		        Map.Entry<String, System> pairs = (Map.Entry<String, Analytics.System>)it.next();
		        devices.add(pairs.getKey());
			}
			jo.put("devices", devices);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jo.toString();
	}
	
	public static String getDeviceInfo(String deviceName) {
		return JsonFormat.printToString(deviceInfo.get(deviceName));
		
	}
	
	public static String getInterfaces(String deviceName) {
		JSONObject jo = new JSONObject();
		List<String> interfacesList = deviceInfo.get(deviceName).getInformation().getInterfaceListList();
		ArrayList<String> interfaces = new ArrayList<String>();
		if(interfacesList != null) {
			try {
				for (String string : interfacesList) {
					interfaces.add(deviceName+":"+string);
				}
				jo.put("interfaces", interfaces);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return jo.toString();
	}
	
	public static String getInterfaceInfo(String interfaceName) {
		Interface interface1 = interfaceInfo.get(interfaceName);
		if(interface1 != null) {
			return JsonFormat.printToString(interface1);
		}
		return "";
	}
	
	public static String getInterfaceQueueStatsChart1(String deviceName, String interfaceName) {
		JSONObject jo = new JSONObject();
		ArrayList<Long> stats = new ArrayList<Long>();
		
		ArrayList<Long> currStats = interfaceQueueStatsChart1.get(interfaceName);
		ArrayList<Long> avgStats = interfaceQueueAvgStatsChart1.get(interfaceName);
		
		if(currStats != null) {
			stats.addAll(currStats);
			for(int i=stats.size();i<300;i++) {
				stats.add(0l);
			}
		}
		try {
			jo.put("queueStats", stats);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		stats = new ArrayList<Long>();
		if(avgStats != null) {
			stats.addAll(avgStats);
			for(int i=stats.size();i<300;i++) {
				stats.add(0l);
			}
		}
		try {
			jo.put("queueAvgStats", stats);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jo.toString();
	}
	
	public static String getInterfaceLatencyStatsChart1(String deviceName, String interfaceName) {
		JSONObject jo = new JSONObject();
		ArrayList<Double> stats = new ArrayList<Double>();
		ArrayList<Double> currStats = interfaceLatencyStatsChart1.get(interfaceName);
		if(currStats != null) {
			stats.addAll(currStats);
			for(int i=stats.size();i<300;i++) {
				stats.add(0.0);
			}
		}
		try {
			jo.put("queueStats", stats);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jo.toString();
	}
	
	public static String getInterfaceQueueStatsInfo(String deviceName, String interfaceName) {
		JSONObject jo = new JSONObject();
		HashMap<Integer, Long> statusInfo = interfaceQueueStatsInfo.get(interfaceName);
		if(statusInfo != null) {
			try {
				jo.put("deviceName", deviceName);
				jo.put("interfaceName", interfaceName);
				jo.put("Current", statusInfo.get(1));
				jo.put("Maximum", statusInfo.get(2));
				jo.put("Minimum", statusInfo.get(3));
				jo.put("Average", statusInfo.get(4));
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return jo.toString();
	}
	
	public static String getInterfaceLatencyStatsInfo(String deviceName, String interfaceName) {
		JSONObject jo = new JSONObject();
		HashMap<Integer, Double> statusInfo = interfaceLatencyStatsInfo.get(interfaceName);
		if(statusInfo != null) {
			try {
				jo.put("deviceName", deviceName);
				jo.put("interfaceName", interfaceName);
				jo.put("Current", statusInfo.get(1));
				jo.put("Maximum", statusInfo.get(2));
				jo.put("Minimum", statusInfo.get(3));
				jo.put("Average", statusInfo.get(4));
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return jo.toString();
	}
	
	public static String getTopInterfacesLatencyInfo(String deviceName) {
		JSONObject jo = new JSONObject();
		try {
			jo.put("topInterfaces", interfaceTopLatencyStatsInfo);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jo.toString();
	}
	
	
	// ============= Receiver code that receives data over a socket ==============

	String host = null;
	int port = -1;

	public JavaQueueReceiver(String host_ , int port_) {
		super(StorageLevel.MEMORY_AND_DISK_2());
		host = host_;
		port = port_;
	}

	public void onStart() {
		// Start the thread that receives data over a connection
		new Thread()  {
			@Override public void run() {
				receive();
			}
		}.start();
	}

	public void onStop() {
		// There is nothing much to do as the thread calling receive()
		// is designed to stop by itself isStopped() returns false
	}

	/** Create a socket connection and receive data until receiver is stopped */
	private void receive() {

		ServerSocket socket = null;
		Socket clientSocket=null;
		String userInput = null;
		try {
			// connect to the server
			socket = new ServerSocket(port);
			clientSocket=socket.accept();
			
			DataInputStream in = new DataInputStream(clientSocket.getInputStream());
			// Until stopped or connection broken continue reading

			while (!isStopped()) {
				try {
					byte[] length = new byte[4];
					length[0] = in.readByte();
					length[1] = in.readByte();
					length[2] = in.readByte();
					length[3] = in.readByte();

					int size = ((length[3]&0xff)<<24)+((length[2]&0xff)<<16)+((length[1]&0xff)<<8)+(length[0]&0xff);

					length[0] = in.readByte();
					length[1] = in.readByte();
					length[2] = in.readByte();
					length[3] = in.readByte(); 
					//CodedInputStream codedIn;
					
					byte[] tmp = new byte[size];
					in.read(tmp);
					//codedIn = CodedInputStream.newInstance(tmp);
					//AnRecord anRecord = AnRecord.parseFrom(codedIn);
					AnRecord anRecord = AnRecord.parseFrom(tmp);
					// Check whether data received is device information.
					if(anRecord.hasTimestamp()) {
						//logger.warn("In queue status");
						store(anRecord);
					} else {
						if(anRecord.hasSystem()) {
							//logger.warn("In device information");
							// Store the device information.
							Analytics.System system = anRecord.getSystem();
							deviceInfo.put(system.getName(), system);
						} else {
							//logger.warn("In interface information");
							// Store the interface information
							for (Interface interface1 : anRecord.getInterfaceList()) {
								interfaceInfo.put(interface1.getName(), interface1);
							}
						}	
					}
				}
				catch (final EOFException e) {
					break;
				}

			}
			in.close();
			// Restart in an attempt to connect again when server is active again
			restart("Trying to connect again");
		} catch(ConnectException ce) {
			// restart if could not connect to server
			restart("Could not connect", ce);
		} catch(Throwable t) {
			restart("Error receiving data", t);
		} finally {
			try {
				clientSocket.close();
				socket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
}
