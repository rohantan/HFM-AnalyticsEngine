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
						if(stats == null) 
							stats = new HashMap<Integer, Long>();
						stats.put(1,interface1.getStats().getQueueStats().getQueueDepth());
						interfaceQueueStatsInfo.put(interface1.getName(), stats);
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
		
		// Print the first ten elements of each RDD generated in this DStream to the console
		//maxStatsPerInterface.print();
		
		maxStatsPerInterface.foreachRDD(
			new Function<JavaPairRDD<String, Long>, Void> () {
				public Void call(JavaPairRDD<String, Long> rdd) {
					for (Tuple2<String, Long> t: rdd.collect()) {
			        	//logger.warn("Maximum status :" + t._1() + ": " + (t._2() / (interfaceInfo.get(t._1()).getStatus().getLink().getSpeed() * 1.0)));
						logger.warn("Maximum status :" + t._1() + ": " + t._2());
						HashMap<String, Long> data = new HashMap<String, Long>();
						HashMap<Integer, Long> stats = interfaceQueueStatsInfo.get(t._1());
						if(stats == null) 
							stats = new HashMap<Integer, Long>();
						stats.put(2,t._2());
						interfaceQueueStatsInfo.put(t._1(), stats);
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
						if(stats == null) 
							stats = new HashMap<Integer, Long>();
						stats.put(3, t._2());
						interfaceQueueStatsInfo.put(t._1(), stats);
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
						if(stats == null) 
							stats = new HashMap<Integer, Long>();
						stats.put(4, (t._2() / (interfaceQueueStats.get(t._1()).size())));
						interfaceQueueStatsInfo.put(t._1(), stats);
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
		Iterator<Entry<String, Interface>> it = interfaceInfo.entrySet().iterator();
		ArrayList<String> interfaces = new ArrayList<String>();
		try {
			while (it.hasNext()) {
		        Map.Entry<String, Interface> pairs = (Map.Entry<String, Interface>)it.next();
		        interfaces.add(pairs.getKey());
			}
			jo.put("interfaces", interfaces);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jo.toString();
	}
	
	public static String getInterfaceInfo(String interfaceName) {
		return JsonFormat.printToString(interfaceInfo.get(interfaceName));
	}
	
	public static String getInterfaceQueueStatsInfo(String deviceName, String interfaceName) {
		JSONObject jo = new JSONObject();
		HashMap<Integer, Long> statusInfo = interfaceQueueStatsInfo.get(interfaceName);
		if(statusInfo != null) {
			try {
				jo.put("deviceName", deviceName);
				jo.put("interfaceName", interfaceName);
				jo.put("current", statusInfo.get(1));
				jo.put("maximum", statusInfo.get(2));
				jo.put("minimum", statusInfo.get(3));
				jo.put("average", statusInfo.get(4));
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
