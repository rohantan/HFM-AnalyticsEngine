package AnalyticsEngine.AE;


import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.Analytics.AnRecord;

import com.google.protobuf.CodedInputStream;


public class SampleProtoBuf {
	public static void main(String[] args) {
		try {
			Logger logger = LoggerFactory.getLogger(SampleProtoBuf.class);
			
			 ServerSocket echoServer = null;
			 Socket clientSocket = null;
			 echoServer = new ServerSocket(50005);
			 clientSocket = echoServer.accept();
			 
			 DataInputStream in = new DataInputStream(clientSocket.getInputStream());
			 
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
		                    in.readFully(tmp,0,size);
		                    codedIn = CodedInputStream.newInstance(tmp);
		                System.out.println("###########SIZE############## "+size);
		                AnRecord anRecord = AnRecord.parseFrom(codedIn);
		                if(!anRecord.hasSystem()){
		                	logger.info(anRecord.toString());
		                }
		                else{
		                	logger.info("non System data!!!!");
		                }
		                // ... do stuff
		            }
		            catch (final EOFException e) {
		                break;
		            }
			 
		        }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
