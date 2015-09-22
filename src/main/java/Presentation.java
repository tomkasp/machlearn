import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.Normalizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.LinkedList;

/**
 * @ Created by A046507 on 9/15/2015.
 */
public class Presentation {
    public static void main(String[] args) {
        //First we need to stark spark context
        //This enables us to use all spark functionality
        SparkConf conf = new SparkConf().setAppName("LeakDetection").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        SQLContext sqlCtx = new SQLContext(sc);

//        Creating a simple RDD
        LinkedList<String> names = new LinkedList<String>();
        names.add("Sam");
        names.add("Dean");
        names.add("Bobby");

        // >>>

        // count
        // print
        // >>>

        // READ IN DATA FROM JSON
        // >>>

        System.out.println("\n\nALL MESSAGES");
        // count
        // print
        // >>>

        // FILTERING
        // FILTER OUT MESSAGES THAT WERE IN FOLDER 'SENT' OR 'ALL DOCUMENTS'
        // >>>

        System.out.println("\n\nVALID FOLDERS");
        // count
        // print
        // >>>

        // DATES
        // FILTER OUT MESSAGES THAT HAVE NO DATE
        // >>>

        System.out.println("\n\nWITH DATES");
        // count
        // print
        // >>>

        // DISTINCT
        // LEAVE ONLY ONE COPY OF EACH MESSAGE
        // >>>
         System.out.println("\n\nDISTINCT");
        // count
        // print
        // >>>

        // MAP
        // UNIFY EMAIL ADDRESSES
        // >>>

        System.out.println("\n\nADDRESS UNIFICATION");
        // count
        // print
        // >>>

        // COMBINE BY KEY
        // CREATE AN RDD WITH A KEY
        // owner will be a key
        // >>>

        System.out.println("\n\nGIVING A KEY TO EACH ENTRY");
        // count
        // print
        // >>>

        // count how many messages each owner has
        // >>>

        System.out.println("\n\nOWNER - NUMBER OF MESSAGES");
        // count
        // print
        // >>>


        System.out.println("\n\nMACHINE LEARNING IN SPARK");

        // first let's have some useful representation of the content of the message
        // >>>

        System.out.println("\n\nCONTENT NO STOP WORDS");
        // count
        // print
        // >>>

        // let's produce TERM FREQUENCY
        // count
        // print
        // >>>

        System.out.println("\n\nWITH TERM FREQUENCIES");
        // count
        // print
        // >>>

        // let's double
        // >>>

        System.out.println("\n\nDOUBLED");
        // count
        // print
        // >>>

        // NORMALISATION
        // cogroup
        // >>>

        System.out.println("\n\nCOGROUPED");
        // count
        // print
        // >>>

        // flatten
        // >>>

        System.out.println("\n\nBOTH VECTORS");
        // count
        // print
        // >>>

        // finally: normalise
        // >>>

        System.out.println("\n\nNORMALISED");
        // count
        // print
        // >>>

    }
}
