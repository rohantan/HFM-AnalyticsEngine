package AnalyticsEngine.AE;

import analytics.Analytics;
import analytics.Analytics.AnRecord;
import analytics.Analytics.Interface;

import com.google.protobuf.CodedInputStream;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.DataInputStream;
import java.io.EOFException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Custom Receiver that receives data over a socket. Received bytes is interpreted as
 * text and \n delimited lines are considered as records. They are then counted and printed.
 *
 * Usage: JavaCustomReceiver <master> <hostname> <port>
 *   <master> is the Spark master URL. In local mode, <master> should be 'local[n]' with n > 1.
 *   <hostname> and <port> of the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.JavaCustomReceiver localhost 9999`
 */

public class JavaTrafficReceiver extends Receiver<AnRecord> {

	public static final Logger logger = LoggerFactory.getLogger(JavaTrafficReceiver.class);
	private static HashMap<String, List<Long>> interfaceTxPcktDrpHM=new HashMap<String, List<Long>>();
	private static HashMap<String, List<Long>> interfaceRxPcktDrpHM=new HashMap<String, List<Long>>();
	public static HashMap<String, Analytics.System> deviceInfo = new HashMap<String, Analytics.System>();
	public static HashMap<String, Interface> interfaceInfo = new HashMap<String, Interface>();
	static HashMap<String, ArrayList<Long>> interfaceQueueStats = new HashMap<String, ArrayList<Long>>();
	static ArrayList<String> deviceMapping = new ArrayList<String>();
	static ArrayList<String> interfaceMapping = new ArrayList<String>();
	private static HashMap<String, Long> interfacePerTxDrpPcktHM = new HashMap<String, Long>();
	private static HashMap<String, Long> interfacePerRxDrpPcktHM = new HashMap<String, Long>();

	public static HashMap<String, Long> getInterfacePerTxDrpPcktHM(String interfaceName) {
		HashMap<String,Long> tempHM= new HashMap<String,Long>();
		tempHM.put(interfaceName, interfacePerTxDrpPcktHM.get(interfaceName));
		return tempHM;
	}

	public static HashMap<String, Long> getInterfacePerRxDrpPcktHM(String interfaceName) {
		HashMap<String,Long> tempHM= new HashMap<String,Long>();
		tempHM.put(interfaceName, interfacePerRxDrpPcktHM.get(interfaceName));
		return tempHM;
	}

	public static void startServer() {
		// Create a local StreamingContext with two working thread and batch interval of 1 second

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("JavaTrafficReceiver");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));
		JavaReceiverInputDStream<AnRecord> lines = jssc.receiverStream(
				new JavaTrafficReceiver("127.0.0.1", 50006));
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<AnRecord, String>() {
			public Iterable<String> call(AnRecord a) {
				if(a.getInterfaceCount()>0){
					List<String> val= new ArrayList<String>();
					for(int i=0;i<a.getInterfaceCount();i++){
						if(a.getInterface(i).hasStats()){
							if(a.getInterface(i).getStats().getTrafficStats().hasTxdroppkt()){
								if(null==interfaceTxPcktDrpHM && null==interfaceRxPcktDrpHM){
									List<Long> tempTxLs=new ArrayList<Long>();
									List<Long> tempRxLs=new ArrayList<Long>();
									tempTxLs.add(a.getInterface(i).getStats().getTrafficStats().getTxdroppkt());
									interfaceTxPcktDrpHM.put(a.getInterface(i).getName(), tempTxLs);
									tempRxLs.add(a.getInterface(i).getStats().getTrafficStats().getRxdroppkt());
									interfaceRxPcktDrpHM.put(a.getInterface(i).getName(), tempRxLs);
								}else if(interfaceTxPcktDrpHM.containsKey(a.getInterface(i).getName()) && interfaceRxPcktDrpHM.containsKey(a.getInterface(i).getName())){
									interfaceTxPcktDrpHM.get(a.getInterface(i).getName()).add(a.getInterface(i).getStats().getTrafficStats().getTxdroppkt());
									interfaceRxPcktDrpHM.get(a.getInterface(i).getName()).add(a.getInterface(i).getStats().getTrafficStats().getRxdroppkt());
								}
								val.add(a.getInterface(i).getName()+","+a.getInterface(i).getStats().getTrafficStats().getTxdroppkt()+";"+a.getInterface(i).getName()+","+a.getInterface(i).getStats().getTrafficStats().getRxdroppkt());
							}
						}
					}
					return val;
				}
				return null;
			}
		});

		//map-reduce for TxDropPkt
		JavaPairDStream<String, Long> TxDropPktsCounts = words.mapToPair(
				new PairFunction<String, String, Long>() {
					public Tuple2<String, Long> call(String s) {
						String tempAr[]=s.split(";",2);
						String tempTxAr[]=tempAr[0].split(",",2);
						return new Tuple2<String, Long>(tempTxAr[0], Long.parseLong(tempTxAr[1]));
					}
				}).reduceByKey(new Function2<Long, Long, Long>() {
					public Long call(Long i1, Long i2) {
						return i1 + i2;
					}
				});

		//iterating and storing the result in hashmap (interfacePerTxDrpPcktHM)
		TxDropPktsCounts.foreachRDD(
				new Function<JavaPairRDD<String, Long>, Void> () {
					public Void call(JavaPairRDD<String, Long> rdd) {
						for (Tuple2<String, Long> t: rdd.collect()) {
							logger.warn("Tx Drop Packets  -> " + t._1() + ": " + t._2());
							interfacePerTxDrpPcktHM.put(t._1(), t._2());
						}
						return null;
					}
				}
				);

		//map-reduce for RxDropPkt
		JavaPairDStream<String, Long> RxDropPktsCounts = words.mapToPair(
				new PairFunction<String, String, Long>() {
					public Tuple2<String, Long> call(String s) {
						String tempAr[]=s.split(";",2);
						String tempTxAr[]=tempAr[0].split(",",2);
						return new Tuple2<String, Long>(tempTxAr[0], Long.parseLong(tempTxAr[1]));
					}
				}).reduceByKey(new Function2<Long, Long, Long>() {
					public Long call(Long i1, Long i2) {
						return i1 + i2;
					}
				});

		//iterating and storing the result in hashmap (interfacePerRxDrpPcktHM)
		RxDropPktsCounts.foreachRDD(
				new Function<JavaPairRDD<String, Long>, Void> () {
					public Void call(JavaPairRDD<String, Long> rdd) {
						for (Tuple2<String, Long> t: rdd.collect()) {
							logger.warn("Rx Drop Packets  -> " + t._1() + ": " + t._2());
							interfacePerRxDrpPcktHM.put(t._1(), t._2());
						}
						return null;
					}
				}
				);

		jssc.start();
		jssc.awaitTermination();
	}

	// ============= Receiver code that receives data over a socket ==============

	String host = null;
	int port = -1;

	public JavaTrafficReceiver(String host_ , int port_) {
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

		try {
			// connect to the server

			Logger logger = LoggerFactory.getLogger(JavaTrafficReceiver.class);

			logger.debug("************STARTING TO STREAM DATA************");
			ServerSocket echoServer = null;
			Socket clientSocket = null;
			echoServer = new ServerSocket(port);
			clientSocket = echoServer.accept();

			DataInputStream in = new DataInputStream(clientSocket.getInputStream());
			// Until stopped or connection broken continue reading
			while (true) {

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

					CodedInputStream codedIn;
					byte[] tmp = new byte[size];
					in.readFully(tmp);
					codedIn = CodedInputStream.newInstance(tmp);
					AnRecord anRecord = AnRecord.parseFrom(codedIn);
					//					
					if(anRecord.hasTimestamp()) {
						store(anRecord);
					} else {
						if(anRecord.hasSystem()) {
							// Store the device information.
							Analytics.System system = anRecord.getSystem();
							deviceInfo.put(system.getName(), system);
							deviceMapping.add(system.getName());
						} else {
							// Store the interface information
							for (Interface interface1 : anRecord.getInterfaceList()) {
								interfaceInfo.put(interface1.getName(), interface1);
								interfaceMapping.add(interface1.getName());
							}
						}	
					}
				}
				catch (final EOFException e) {
					break;
				}
			}
			in.close();
			echoServer.close();
			clientSocket.close();

			// Restart in an attempt to connect again when server is active again
			restart("Trying to connect again");
		} catch(ConnectException ce) {
			// restart if could not connect to server
			restart("Could not connect", ce);
		} catch(Throwable t) {
			restart("Error receiving data", t);
		}
	}
}
