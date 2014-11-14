package AnalyticsEngine.AE;

/*import java.util.ArrayList;
import java.util.List;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import scala.Array;
import scala.Tuple2;
*/

public class App 
{
    public static void main( String[] args )
    {
    	String s="Hi,Rohan;hi,Rat;hi,ron;hi,nahor";
    	String tempAr[]=s.split(";",4);
    	System.out.println(tempAr.length);
    	String tempAr1[]=tempAr[3].split(",",2);
    	System.out.println(tempAr1[0]+"    "+tempAr1[1]);
    	/*String temp= "analytics-qfx5100-02:xe-0\/0\/51";
    	temp=temp.replace("\", "");
    	System.out.println(temp);*/
    }
}
