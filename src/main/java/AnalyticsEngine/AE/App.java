package AnalyticsEngine.AE;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import scala.Array;
import scala.Tuple2;


public class App 
{
    public static void main( String[] args )
    {
    	String s="Hi,Rohan";
    	String tempAr[]=s.split(",",2);
    	System.out.println(tempAr[0]+"    "+tempAr[1]);
    }
}
