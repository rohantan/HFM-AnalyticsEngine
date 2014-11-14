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
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
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

	private static final int NOOFPARTS=9;
	private static List<String> crcval= new ArrayList<String>();
	private static List<String> totaltxdrpval= new ArrayList<String>();
	private static List<String> totalrxdrpval= new ArrayList<String>();
	private static boolean flag=false;

	public static final Logger logger = LoggerFactory.getLogger(JavaTrafficReceiver.class);
	private static HashMap<String, List<Long>> interfaceTxPcktDrpHM=new HashMap<String, List<Long>>();
	private static HashMap<String, List<Long>> interfaceRxPcktDrpHM=new HashMap<String, List<Long>>();
	private static HashMap<String, List<Long>> interfaceTxPpsHM=new HashMap<String, List<Long>>();
	private static HashMap<String, List<Long>> interfaceRxPpsHM=new HashMap<String, List<Long>>();
	private static HashMap<String, List<Long>> interfaceTxBpsHM=new HashMap<String, List<Long>>();
	private static HashMap<String, List<Long>> interfaceRxBpsHM=new HashMap<String, List<Long>>();
	private static HashMap<String, List<Long>> interfaceTxPcktsHM=new HashMap<String, List<Long>>();
	private static HashMap<String, List<Long>> interfaceRxPcktsHM=new HashMap<String, List<Long>>();
	private static HashMap<String, List<Long>> interfaceRxCrcErrHM=new HashMap<String, List<Long>>();

	public static HashMap<String, Analytics.System> deviceInfo = new HashMap<String, Analytics.System>();
	public static HashMap<String, Interface> interfaceInfo = new HashMap<String, Interface>();
	static HashMap<String, ArrayList<Long>> interfaceQueueStats = new HashMap<String, ArrayList<Long>>();
	static ArrayList<String> deviceMapping = new ArrayList<String>();
	static ArrayList<String> interfaceMapping = new ArrayList<String>();

	private static HashMap<String, Long> interfacePerTxDrpPcktHM = new HashMap<String, Long>();
	private static HashMap<String, Long> interfacePerRxDrpPcktHM = new HashMap<String, Long>();
	private static HashMap<String, Long> interfacePerTxPktPerSecHM = new HashMap<String, Long>();
	private static HashMap<String, Long> interfacePerRxPktPerSecHM = new HashMap<String, Long>();
	private static HashMap<String, Long> interfacePerTxBytPerSecHM= new HashMap<String, Long>();
	private static HashMap<String, Long> interfacePerRxBytPerSecHM= new HashMap<String, Long>();
	private static HashMap<String, Long> interfacePerTxPktsHM= new HashMap<String, Long>();
	private static HashMap<String, Long> interfacePerRxPktsHM= new HashMap<String, Long>();
	private static HashMap<String, Long> interfacePerRxCrcErrHM= new HashMap<String, Long>();
	private static HashMap<String, Long> interfacePerRxTotalCrcErrHM= new HashMap<String, Long>();
	private static HashMap<String, Long> interfacePerTxTotalDrpPktHM= new HashMap<String, Long>();
	private static HashMap<String, Long> interfacePerRxTotalDrpPktHM= new HashMap<String, Long>();

	public static HashMap<String, Long> getInterfacePerTxTotalDrpPktHM(String interfaceName) {
		HashMap<String,Long> tempHM= new HashMap<String,Long>();
		tempHM.put(interfaceName,interfacePerTxTotalDrpPktHM.get(interfaceName));
		return tempHM;
	}

	public static HashMap<String, Long> getInterfacePerRxTotalDrpPktHM(String interfaceName) {
		HashMap<String,Long> tempHM= new HashMap<String,Long>();
		tempHM.put(interfaceName,interfacePerRxTotalDrpPktHM.get(interfaceName));
		return tempHM;
	}

	public static HashMap<String, Long> getInterfacePerRxTotalCrcErrHM(String interfaceName) {
		HashMap<String,Long> tempHM= new HashMap<String,Long>();
		tempHM.put(interfaceName,interfacePerRxTotalCrcErrHM.get(interfaceName));
		return tempHM;
	}

	public static HashMap<String, Long> getInterfacePerRxCrcErrHM(String interfaceName) {
		HashMap<String,Long> tempHM= new HashMap<String,Long>();
		tempHM.put(interfaceName,interfacePerRxCrcErrHM.get(interfaceName));
		return tempHM;
	}

	public static HashMap<String, Long> getInterfacePerTxPktsHM(String interfaceName) {
		HashMap<String,Long> tempHM= new HashMap<String,Long>();
		tempHM.put(interfaceName,interfacePerTxPktsHM.get(interfaceName));
		return tempHM;
	}

	public static HashMap<String, Long> getInterfacePerRxPktsHM(String interfaceName) {
		HashMap<String,Long> tempHM= new HashMap<String,Long>();
		tempHM.put(interfaceName,interfacePerRxPktsHM.get(interfaceName));
		return tempHM;
	}

	public static HashMap<String, Long> getInterfacePerTxBytPerSecHM(String interfaceName) {
		HashMap<String,Long> tempHM= new HashMap<String,Long>();
		tempHM.put(interfaceName,interfacePerTxBytPerSecHM.get(interfaceName));
		return tempHM;
	}

	public static HashMap<String, Long> getInterfacePerRxBytPerSecHM(String interfaceName) {
		HashMap<String,Long> tempHM= new HashMap<String,Long>();
		tempHM.put(interfaceName,interfacePerRxBytPerSecHM.get(interfaceName));
		return tempHM;
	}

	public static HashMap<String, Long> getInterfacePerTxPktPerSecHM(String interfaceName) {
		HashMap<String,Long> tempHM= new HashMap<String,Long>();
		tempHM.put(interfaceName,interfacePerTxPktPerSecHM.get(interfaceName));
		return tempHM;
	}

	public static HashMap<String, Long> getInterfacePerRxPktPerSecHM(String interfaceName) {
		HashMap<String,Long> tempHM= new HashMap<String,Long>();
		tempHM.put(interfaceName,interfacePerRxPktPerSecHM.get(interfaceName));
		return tempHM;
	}

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

	//get continuous graph for pckt drops
	public static String getInterfaceTxPcktDrpHM(String interfaceName) {
		JSONObject jo = new JSONObject();
		List<Long> stats = new ArrayList<Long>();

		List<Long> interfaceTxPcktDrpStats = interfaceTxPcktDrpHM.get(interfaceName);
		List<Long> interfaceRxPcktDrpStats = interfaceRxPcktDrpHM.get(interfaceName);

		if(interfaceTxPcktDrpStats != null) {
			stats.addAll(interfaceTxPcktDrpStats);
			for(int i=stats.size();i<300;i++) {
				stats.add(0l);
			}
		}
		try {
			jo.put("interfaceTxDrpPckt", stats);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		stats = new ArrayList<Long>();
		if(interfaceRxPcktDrpStats != null) {
			stats.addAll(interfaceRxPcktDrpStats);
			for(int i=stats.size();i<300;i++) {
				stats.add(0l);
			}
		}
		try {
			jo.put("interfaceRxDrpPckt", stats);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return jo.toString();
	}

	//get continuous graph for pps
	public static String getInterfaceTxPpsHM(String interfaceName) {
		JSONObject jo = new JSONObject();
		List<Long> stats = new ArrayList<Long>();

		List<Long> interfaceTxPpsHMStats = interfaceTxPpsHM.get(interfaceName);
		List<Long> interfaceRxPpsHMStats = interfaceRxPpsHM.get(interfaceName);

		if(interfaceTxPpsHM != null) {
			stats.addAll(interfaceTxPpsHMStats);
			for(int i=stats.size();i<300;i++) {
				stats.add(0l);
			}
		}
		try {
			jo.put("interfaceTxPps", stats);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		stats = new ArrayList<Long>();
		if(interfaceRxPpsHM != null) {
			stats.addAll(interfaceRxPpsHMStats);
			for(int i=stats.size();i<300;i++) {
				stats.add(0l);
			}
		}
		try {
			jo.put("interfaceRxPps", stats);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return jo.toString();
	}

	//get continuous graph for bps
	public static String getInterfaceTxBpsHM(String interfaceName) {
		JSONObject jo = new JSONObject();
		List<Long> stats = new ArrayList<Long>();

		List<Long> interfaceTxBpsHMStats = interfaceTxBpsHM.get(interfaceName);
		List<Long> interfaceRxBpsHMStats = interfaceRxBpsHM.get(interfaceName);

		if(interfaceTxBpsHM != null) {
			stats.addAll(interfaceTxBpsHMStats);
			for(int i=stats.size();i<300;i++) {
				stats.add(0l);
			}
		}
		try {
			jo.put("interfaceTxBps", stats);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		stats = new ArrayList<Long>();
		if(interfaceRxBpsHM != null) {
			stats.addAll(interfaceRxBpsHMStats);
			for(int i=stats.size();i<300;i++) {
				stats.add(0l);
			}
		}
		try {
			jo.put("interfaceRxBps", stats);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return jo.toString();
	}
	
	
	public static void startServer() {
		// Create a local StreamingContext with two working thread and batch interval of 1 second

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("JavaTrafficReceiver");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));
		JavaReceiverInputDStream<AnRecord> lines = jssc.receiverStream(
				new JavaTrafficReceiver("127.0.0.1", 50006));
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<AnRecord, String>() {
			public Iterable<String> call(AnRecord a) {
				if(flag){
					flag=false;
					if(a.getInterfaceCount()>0){
						List<String> val= new ArrayList<String>();
						for(int i=0;i<a.getInterfaceCount();i++){
							if(a.getInterface(i).hasStats()){
								if(a.getInterface(i).getStats().getTrafficStats().hasTxdroppkt()){
									List<Long> tempTxLs=interfaceTxPcktDrpHM.get(a.getInterface(i).getName());
									List<Long> tempRxLs=interfaceRxPcktDrpHM.get(a.getInterface(i).getName());
									if(null==tempTxLs && null==tempRxLs){
										tempTxLs=new ArrayList<Long>();
										tempRxLs=new ArrayList<Long>();
									}
									tempTxLs.add(a.getInterface(i).getStats().getTrafficStats().getTxdroppkt());
									interfaceTxPcktDrpHM.put(a.getInterface(i).getName(), tempTxLs);
									tempRxLs.add(a.getInterface(i).getStats().getTrafficStats().getRxdroppkt());
									interfaceRxPcktDrpHM.put(a.getInterface(i).getName(), tempRxLs);



									List<Long> tempTxPpsLs=interfaceTxPpsHM.get(a.getInterface(i).getName());
									List<Long> tempRxPpsLs=interfaceRxPpsHM.get(a.getInterface(i).getName());
									if(null==tempTxPpsLs && null==tempRxPpsLs){
										tempTxPpsLs=new ArrayList<Long>();
										tempRxPpsLs=new ArrayList<Long>();
									}
									tempTxPpsLs.add(a.getInterface(i).getStats().getTrafficStats().getTxpps());
									interfaceTxPpsHM.put(a.getInterface(i).getName(), tempTxPpsLs);
									tempRxPpsLs.add(a.getInterface(i).getStats().getTrafficStats().getRxpps());
									interfaceRxPpsHM.put(a.getInterface(i).getName(), tempRxPpsLs);

									List<Long> tempTxBpsLs=interfaceTxBpsHM.get(a.getInterface(i).getName());
									List<Long> tempRxBpsLs=interfaceRxBpsHM.get(a.getInterface(i).getName());
									if(null==tempTxBpsLs && null==tempRxBpsLs){
										tempTxBpsLs=new ArrayList<Long>();
										tempRxBpsLs=new ArrayList<Long>();
									}
									tempTxBpsLs.add(a.getInterface(i).getStats().getTrafficStats().getTxbps());
									interfaceTxBpsHM.put(a.getInterface(i).getName(), tempTxBpsLs);
									tempRxBpsLs.add(a.getInterface(i).getStats().getTrafficStats().getRxbps());
									interfaceRxBpsHM.put(a.getInterface(i).getName(), tempRxBpsLs);

									List<Long> tempTxPktsLs=interfaceTxPcktsHM.get(a.getInterface(i).getName());
									List<Long> tempRxPktsLs=interfaceRxPcktsHM.get(a.getInterface(i).getName());
									if(null==tempTxPktsLs && null==tempRxPktsLs){
										tempTxPktsLs=new ArrayList<Long>();
										tempRxPktsLs=new ArrayList<Long>();
									}
									tempTxPktsLs.add(a.getInterface(i).getStats().getTrafficStats().getTxpkt());
									interfaceTxPcktsHM.put(a.getInterface(i).getName(), tempTxPktsLs);
									tempRxPktsLs.add(a.getInterface(i).getStats().getTrafficStats().getRxpkt());
									interfaceRxPcktsHM.put(a.getInterface(i).getName(), tempRxPktsLs);

									List<Long> tempRxCrcErrLs=interfaceRxCrcErrHM.get(a.getInterface(i).getName());
									if(null==tempRxCrcErrLs){
										tempRxCrcErrLs=new ArrayList<Long>();
									}
									tempRxCrcErrLs.add(a.getInterface(i).getStats().getTrafficStats().getRxcrcerr());
									interfaceRxCrcErrHM.put(a.getInterface(i).getName(), tempRxCrcErrLs);

									val.add(a.getInterface(i).getName()+","+a.getInterface(i).getStats().getTrafficStats().getTxdroppkt()+";"
											+a.getInterface(i).getName()+","+a.getInterface(i).getStats().getTrafficStats().getRxdroppkt()+";"
											+a.getInterface(i).getName()+","+a.getInterface(i).getStats().getTrafficStats().getTxpps()+";"
											+a.getInterface(i).getName()+","+a.getInterface(i).getStats().getTrafficStats().getRxpps()+";"
											+a.getInterface(i).getName()+","+a.getInterface(i).getStats().getTrafficStats().getTxbps()+";"
											+a.getInterface(i).getName()+","+a.getInterface(i).getStats().getTrafficStats().getRxbps()+";"
											+a.getInterface(i).getName()+","+a.getInterface(i).getStats().getTrafficStats().getTxpkt()+";"
											+a.getInterface(i).getName()+","+a.getInterface(i).getStats().getTrafficStats().getRxpkt()+";"
											+a.getInterface(i).getName()+","+a.getInterface(i).getStats().getTrafficStats().getRxcrcerr());
								}
							}
						}
						return val;
					}
				}
				return null;
			}
		});

		//map-reduce for TxDropPkt
		JavaPairDStream<String, Long> TxDropPktsCounts = words.mapToPair(
				new PairFunction<String, String, Long>() {
					public Tuple2<String, Long> call(String s) {
						String tempAr[]=s.split(";",NOOFPARTS);
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
						String tempAr[]=s.split(";",NOOFPARTS);
						String tempTxAr[]=tempAr[1].split(",",2);
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


		//map-reduce for TxPps
		JavaPairDStream<String, Long> TxPpsCounts = words.mapToPair(
				new PairFunction<String, String, Long>() {
					public Tuple2<String, Long> call(String s) {
						String tempAr[]=s.split(";",NOOFPARTS);
						String tempTxAr[]=tempAr[2].split(",",2);
						return new Tuple2<String, Long>(tempTxAr[0], Long.parseLong(tempTxAr[1]));
					}
				}).reduceByKey(new Function2<Long, Long, Long>() {
					public Long call(Long i1, Long i2) {
						return i1 + i2;
					}
				});

		//iterating and storing the result in hashmap (interfacePerTxPktPerSecHM)
		TxPpsCounts.foreachRDD(
				new Function<JavaPairRDD<String, Long>, Void> () {
					public Void call(JavaPairRDD<String, Long> rdd) {
						for (Tuple2<String, Long> t: rdd.collect()) {
							logger.warn("Tx Packets Per Sec -> " + t._1() + ": " + t._2());
							interfacePerTxPktPerSecHM.put(t._1(), t._2());
						}
						return null;
					}
				}
				);

		//map-reduce for RxPps
		JavaPairDStream<String, Long> RxPpsCounts = words.mapToPair(
				new PairFunction<String, String, Long>() {
					public Tuple2<String, Long> call(String s) {
						String tempAr[]=s.split(";",NOOFPARTS);
						String tempTxAr[]=tempAr[3].split(",",2);
						return new Tuple2<String, Long>(tempTxAr[0], Long.parseLong(tempTxAr[1]));
					}
				}).reduceByKey(new Function2<Long, Long, Long>() {
					public Long call(Long i1, Long i2) {
						return i1 + i2;
					}
				});

		//iterating and storing the result in hashmap (interfacePerRxPktPerSecHM)
		RxPpsCounts.foreachRDD(
				new Function<JavaPairRDD<String, Long>, Void> () {
					public Void call(JavaPairRDD<String, Long> rdd) {
						for (Tuple2<String, Long> t: rdd.collect()) {
							logger.warn("Rx Packets Per Sec -> " + t._1() + ": " + t._2());
							interfacePerRxPktPerSecHM.put(t._1(), t._2());
						}
						return null;
					}
				}
				);

		//map-reduce for TxBps
		JavaPairDStream<String, Long> TxBpsCounts = words.mapToPair(
				new PairFunction<String, String, Long>() {
					public Tuple2<String, Long> call(String s) {
						String tempAr[]=s.split(";",NOOFPARTS);
						String tempTxAr[]=tempAr[4].split(",",2);
						return new Tuple2<String, Long>(tempTxAr[0], Long.parseLong(tempTxAr[1]));
					}
				}).reduceByKey(new Function2<Long, Long, Long>() {
					public Long call(Long i1, Long i2) {
						return i1 + i2;
					}
				});

		//iterating and storing the result in hashmap (interfacePerTxBytPerSecHM)
		TxBpsCounts.foreachRDD(
				new Function<JavaPairRDD<String, Long>, Void> () {
					public Void call(JavaPairRDD<String, Long> rdd) {
						for (Tuple2<String, Long> t: rdd.collect()) {
							logger.warn("Tx Bytes Per Sec -> " + t._1() + ": " + t._2());
							interfacePerTxBytPerSecHM.put(t._1(), t._2());
						}
						return null;
					}
				}
				);

		//map-reduce for RxBps
		JavaPairDStream<String, Long> RxBpsCounts = words.mapToPair(
				new PairFunction<String, String, Long>() {
					public Tuple2<String, Long> call(String s) {
						String tempAr[]=s.split(";",NOOFPARTS);
						String tempTxAr[]=tempAr[5].split(",",2);
						return new Tuple2<String, Long>(tempTxAr[0], Long.parseLong(tempTxAr[1]));
					}
				}).reduceByKey(new Function2<Long, Long, Long>() {
					public Long call(Long i1, Long i2) {
						return i1 + i2;
					}
				});

		//iterating and storing the result in hashmap (interfacePerRxBytPerSecHM)
		RxBpsCounts.foreachRDD(
				new Function<JavaPairRDD<String, Long>, Void> () {
					public Void call(JavaPairRDD<String, Long> rdd) {
						for (Tuple2<String, Long> t: rdd.collect()) {
							logger.warn("Rx Bytes Per Sec -> " + t._1() + ": " + t._2());
							interfacePerRxBytPerSecHM.put(t._1(), t._2());
						}
						return null;
					}
				}
				);


		//map-reduce for TxPckts
		JavaPairDStream<String, Long> TxPcktsCounts = words.mapToPair(
				new PairFunction<String, String, Long>() {
					public Tuple2<String, Long> call(String s) {
						String tempAr[]=s.split(";",NOOFPARTS);
						String tempTxAr[]=tempAr[6].split(",",2);
						return new Tuple2<String, Long>(tempTxAr[0], Long.parseLong(tempTxAr[1]));
					}
				}).reduceByKey(new Function2<Long, Long, Long>() {
					public Long call(Long i1, Long i2) {
						return i1 + i2;
					}
				});

		//iterating and storing the result in hashmap (interfacePerTxPktsHM)
		TxPcktsCounts.foreachRDD(
				new Function<JavaPairRDD<String, Long>, Void> () {
					public Void call(JavaPairRDD<String, Long> rdd) {
						for (Tuple2<String, Long> t: rdd.collect()) {
							logger.warn("Tx Packets -> " + t._1() + ": " + t._2());
							interfacePerTxPktsHM.put(t._1(), t._2());
						}
						return null;
					}
				}
				);

		//map-reduce for RxPckts
		JavaPairDStream<String, Long> RxPcktsCounts = words.mapToPair(
				new PairFunction<String, String, Long>() {
					public Tuple2<String, Long> call(String s) {
						String tempAr[]=s.split(";",NOOFPARTS);
						String tempTxAr[]=tempAr[7].split(",",2);
						return new Tuple2<String, Long>(tempTxAr[0], Long.parseLong(tempTxAr[1]));
					}
				}).reduceByKey(new Function2<Long, Long, Long>() {
					public Long call(Long i1, Long i2) {
						return i1 + i2;
					}
				});

		//iterating and storing the result in hashmap (interfacePerRxPktsHM)
		RxPcktsCounts.foreachRDD(
				new Function<JavaPairRDD<String, Long>, Void> () {
					public Void call(JavaPairRDD<String, Long> rdd) {
						for (Tuple2<String, Long> t: rdd.collect()) {
							logger.warn("Rx Packets -> " + t._1() + ": " + t._2());
							interfacePerRxPktsHM.put(t._1(), t._2());
						}
						return null;
					}
				}
				);

		//map-reduce for RxCrcErr
		JavaPairDStream<String, Long> RxCrcErrCounts = words.mapToPair(
				new PairFunction<String, String, Long>() {
					public Tuple2<String, Long> call(String s) {
						String tempAr[]=s.split(";",NOOFPARTS);
						String tempTxAr[]=tempAr[8].split(",",2);
						return new Tuple2<String, Long>(tempTxAr[0], Long.parseLong(tempTxAr[1]));
					}
				}).reduceByKey(new Function2<Long, Long, Long>() {
					public Long call(Long i1, Long i2) {
						return i1 + i2;
					}
				});

		//iterating and storing the result in hashmap (interfacePerRxCrcErrHM)
		RxCrcErrCounts.foreachRDD(
				new Function<JavaPairRDD<String, Long>, Void> () {
					public Void call(JavaPairRDD<String, Long> rdd) {
						for (Tuple2<String, Long> t: rdd.collect()) {
							logger.warn("Rx Crc Errors -> " + t._1() + ": " + t._2());
							interfacePerRxCrcErrHM.put(t._1(), t._2());
						}
						return null;
					}
				}
				);

		//extracting totalcrcerror counts from the stream 
		JavaDStream<String> totalcrcerrors = lines.flatMap(new FlatMapFunction<AnRecord, String>() {
			public Iterable<String> call(AnRecord a) {
				if(a.getInterfaceCount()>0){
					for(int i=0;i<a.getInterfaceCount();i++){
						if(a.getInterface(i).hasStats()){
							if(a.getInterface(i).getStats().getTrafficStats().hasRxcrcerr()){
								crcval.add(a.getInterface(i).getName()+","+a.getInterface(i).getStats().getTrafficStats().getRxcrcerr());
							}
						}
					}
					return crcval;
				}
				return null;
			}
		});		

		//map-reduce for TotalRxCrcErr
		JavaPairDStream<String, Long> RxTotalCrcErrCounts = totalcrcerrors.mapToPair(
				new PairFunction<String, String, Long>() {
					public Tuple2<String, Long> call(String s) {
						String tempAr[]=s.split(",",2);
						return new Tuple2<String, Long>(tempAr[0], Long.parseLong(tempAr[1]));
					}
				}).reduceByKey(new Function2<Long, Long, Long>() {
					public Long call(Long i1, Long i2) {
						return i1 + i2;
					}
				});

		//iterating and storing the result in hashmap (interfacePerRxTotalCrcErrHM)
		RxTotalCrcErrCounts.foreachRDD(
				new Function<JavaPairRDD<String, Long>, Void> () {
					public Void call(JavaPairRDD<String, Long> rdd) {
						for (Tuple2<String, Long> t: rdd.collect()) {
							logger.warn("Total Rx Crc Errors -> " + t._1() + ": " + t._2());
							interfacePerRxTotalCrcErrHM.put(t._1(), t._2());
						}
						return null;
					}
				}
				);

		//extracting totaltxdroppckts counts from the stream 
		JavaDStream<String> totaltxdrppckts = lines.flatMap(new FlatMapFunction<AnRecord, String>() {
			public Iterable<String> call(AnRecord a) {
				if(a.getInterfaceCount()>0){
					for(int i=0;i<a.getInterfaceCount();i++){
						if(a.getInterface(i).hasStats()){
							if(a.getInterface(i).getStats().getTrafficStats().hasRxcrcerr()){
								totaltxdrpval.add(a.getInterface(i).getName()+","+a.getInterface(i).getStats().getTrafficStats().getTxdroppkt());
							}
						}
					}
					return totaltxdrpval;
				}
				return null;
			}
		});		

		//map-reduce for TotalTxDropPckts
		JavaPairDStream<String, Long> TxTotalDrpPcktsCounts = totaltxdrppckts.mapToPair(
				new PairFunction<String, String, Long>() {
					public Tuple2<String, Long> call(String s) {
						String tempAr[]=s.split(",",2);
						return new Tuple2<String, Long>(tempAr[0], Long.parseLong(tempAr[1]));
					}
				}).reduceByKey(new Function2<Long, Long, Long>() {
					public Long call(Long i1, Long i2) {
						return i1 + i2;
					}
				});

		//iterating and storing the result in hashmap (interfacePerTxTotalDrpPktHM)
		TxTotalDrpPcktsCounts.foreachRDD(
				new Function<JavaPairRDD<String, Long>, Void> () {
					public Void call(JavaPairRDD<String, Long> rdd) {
						for (Tuple2<String, Long> t: rdd.collect()) {
							logger.warn("Total Tx Drop Packets till now -> " + t._1() + ": " + t._2());
							interfacePerTxTotalDrpPktHM.put(t._1(), t._2());
						}
						return null;
					}
				}
				);

		//extracting totalrxdroppckts counts from the stream 
		JavaDStream<String> totalrxdrppckts = lines.flatMap(new FlatMapFunction<AnRecord, String>() {
			public Iterable<String> call(AnRecord a) {
				if(a.getInterfaceCount()>0){
					for(int i=0;i<a.getInterfaceCount();i++){
						if(a.getInterface(i).hasStats()){
							if(a.getInterface(i).getStats().getTrafficStats().hasRxcrcerr()){
								totalrxdrpval.add(a.getInterface(i).getName()+","+a.getInterface(i).getStats().getTrafficStats().getRxdroppkt());
							}
						}
					}
					return totalrxdrpval;
				}
				return null;
			}
		});		

		//map-reduce for TotalRxDropPckts
		JavaPairDStream<String, Long> RxTotalDrpPcktsCounts = totalrxdrppckts.mapToPair(
				new PairFunction<String, String, Long>() {
					public Tuple2<String, Long> call(String s) {
						String tempAr[]=s.split(",",2);
						return new Tuple2<String, Long>(tempAr[0], Long.parseLong(tempAr[1]));
					}
				}).reduceByKey(new Function2<Long, Long, Long>() {
					public Long call(Long i1, Long i2) {
						return i1 + i2;
					}
				});

		//iterating and storing the result in hashmap (interfacePerRxTotalDrpPktHM)
		RxTotalDrpPcktsCounts.foreachRDD(
				new Function<JavaPairRDD<String, Long>, Void> () {
					public Void call(JavaPairRDD<String, Long> rdd) {
						for (Tuple2<String, Long> t: rdd.collect()) {
							logger.warn("Total Rx Drop Packets till now -> " + t._1() + ": " + t._2());
							interfacePerRxTotalDrpPktHM.put(t._1(), t._2());
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
						flag=true;
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
