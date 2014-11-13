package AnalyticsEngine.AE;

import analytics.Analytics.AnRecord;

import com.google.common.collect.Lists;
import com.google.protobuf.CodedInputStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
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
import java.util.regex.Pattern;

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

public class JavaCustomReceiver extends Receiver<AnRecord> {
	private static final Pattern SPACE = Pattern.compile(" ");
	private static HashMap<String, List<Long>> interfaceTxPcktDrpHM=new HashMap<String, List<Long>>();
	public static void main(String[] args) {
//		val.add(0);
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("JavaCustomReceiver");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));
		JavaReceiverInputDStream<AnRecord> lines = jssc.receiverStream(
				new JavaCustomReceiver("127.0.0.1", 50006));
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<AnRecord, String>() {
        	public Iterable<String> call(AnRecord a) {
        		if(a.getInterfaceCount()>0){
        			List<String> val= new ArrayList<String>();
        			for(int i=0;i<a.getInterfaceCount();i++){
        				if(a.getInterface(i).hasStats()){
        					if(a.getInterface(i).getStats().getTrafficStats().hasTxdroppkt()){
        						if(null==interfaceTxPcktDrpHM){
            						List<Long> tempLs=new ArrayList<Long>();
                					tempLs.add(a.getInterface(i).getStats().getTrafficStats().getTxdroppkt());
            						interfaceTxPcktDrpHM.put(a.getInterface(i).getName(), tempLs);
            					}else if(interfaceTxPcktDrpHM.containsKey(a.getInterface(i).getName())){
            						interfaceTxPcktDrpHM.get(a.getInterface(i).getName()).add(a.getInterface(i).getStats().getTrafficStats().getTxdroppkt());
            					}
            					val.add(a.getInterface(i).getName()+","+a.getInterface(i).getStats().getTrafficStats().getTxdroppkt());
        					}
        				}
        			}
        			return val;
        		}
        		return Lists.newArrayList("none");
        	}
        });
//		if(null!=interfaceTxPcktDrpHM && !interfaceTxPcktDrpHM.isEmpty())
			JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
					new PairFunction<String, String, Integer>() {
						public Tuple2<String, Integer> call(String s) {
							String tempAr[]=s.split(",",2);
							return new Tuple2<String, Integer>(tempAr[0], Integer.parseInt(tempAr[1]));
						}
					}).reduceByKey(new Function2<Integer, Integer, Integer>() {
						public Integer call(Integer i1, Integer i2) {
							return i1 + i2;
						}
					});
			System.out.println("***********");
			wordCounts.print();
			System.out.println("***********");
//		words.print();
//		lines.print();
		jssc.start();
		jssc.awaitTermination();
	}

	// ============= Receiver code that receives data over a socket ==============

	String host = null;
	int port = -1;

	public JavaCustomReceiver(String host_ , int port_) {
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
			
			Logger logger = LoggerFactory.getLogger(JavaCustomReceiver.class);
			
			logger.debug("************STARTING TO STREAM************");
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
//					logger.info(anRecord.toString());
					/*if(!anRecord.hasSystem()){
	                for(int i=0;i<100;i++){
	                	if(!anRecord.getInterface(0).hasInformation() && !anRecord.getInterface(0).hasStatus()){
	                		logger.debug("Interface: "+i);
		                	logger.debug(""+anRecord.getInterfaceList().get(i).getStats().getTrafficStats());
	                	}
	                }
                }*/	
					/*logger.debug("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ "+anRecord.toString());
					if(anRecord.hasSystem()){
						logger.debug("has SYSYEM!!!!!!!!!!!!!!!!");
					}
					else if(!anRecord.hasSystem()){
						logger.debug("not System!!!!!");
						store(anRecord);
					}*/
					if(anRecord.hasTimestamp())
						store(anRecord);

					// ... do stuff
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
