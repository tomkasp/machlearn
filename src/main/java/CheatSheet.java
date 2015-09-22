import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.Normalizer;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

/**
 * @author Agnieszka Pocha
 * Created by A046507 on 9/11/2015.
 */
public class CheatSheet {

    public static void main(final String[] args) {
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

        JavaRDD<String> namesRDD = sc.parallelize(names);

        System.out.println(namesRDD);

        namesRDD.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });



        // READ IN DATA FROM JSON
        JavaRDD<MessageEntry> messages = sqlCtx.read().json("messages.json").toJavaRDD().map(new Function<Row, MessageEntry>() {
            public MessageEntry call(Row row) throws Exception {
                return Utils.rowToMessageEntry(row);
            }
        });

        System.out.println("ALL MESSAGES");
        System.out.println(messages.count());

        messages.foreach(new VoidFunction<MessageEntry>() {
            public void call(MessageEntry messageEntry) throws Exception {
                System.out.println(messageEntry);
            }
        });


        // FILTERING

        // FILTER OUT MESSAGES THAT WERE IN FOLDER 'SENT' OR 'ALL DOCUMENTS'
        JavaRDD<MessageEntry> validFolders = messages.filter(new Function<MessageEntry, Boolean>() {
            public Boolean call(MessageEntry messageEntry) throws Exception {
                return !(messageEntry.getFolder().contains("Sent") || messageEntry.getFolder().contains("All documents"));
            }
        });

        System.out.println("VALID FOLDERS");
        System.out.println(validFolders.count());

        validFolders.foreach(new VoidFunction<MessageEntry>() {
            public void call(MessageEntry messageEntry) throws Exception {
                System.out.println(messageEntry);
            }
        });

        // DATES
        // FILTER OUT MESSAGES THAT HAVE NO DATE
        JavaRDD<MessageEntry> withDates = validFolders.filter(new Function<MessageEntry, Boolean>() {
            public Boolean call(MessageEntry messageEntry) throws Exception {
                return !Utils.isStringEmpty(messageEntry.getDate());
            }
        });
        System.out.println("WITH DATES");
        System.out.println(withDates.count());

        withDates.foreach(new VoidFunction<MessageEntry>() {
            public void call(MessageEntry messageEntry) throws Exception {
                System.out.println(messageEntry);
            }
        });


        // DISTINCT
        // LEAVE ONLY ONE COPY OF EACH MESSAGE
        JavaRDD<MessageEntry> distinct = withDates.distinct();
        System.out.println("DISTINCT");
        System.out.println(distinct.count());

        distinct.foreach(new VoidFunction<MessageEntry>() {
            public void call(MessageEntry messageEntry) throws Exception {
                System.out.println(messageEntry);
            }
        });

        // MAP
        // UNIFY EMAIL ADDRESSES
        JavaRDD<MessageEntry> unified = distinct.map(new Function<MessageEntry, MessageEntry>() {
            public MessageEntry call(MessageEntry messageEntry) throws Exception {
                return Utils.unify(messageEntry);
            }
        });

        System.out.println("ADDRESS UNIFICATION");
        System.out.println(unified.count());

        unified.foreach(new VoidFunction<MessageEntry>() {
            public void call(MessageEntry messageEntry) throws Exception {
                System.out.println(messageEntry);
            }
        });

        // COMBINE BY KEY
        // CREATE AN RDD WITH A KEY
        // owner will be a key
        JavaPairRDD<String, MessageEntry> ownerMessage = unified.mapToPair(new PairFunction<MessageEntry, String, MessageEntry>() {
            public Tuple2<String, MessageEntry> call(MessageEntry messageEntry) throws Exception {
                return new Tuple2<String, MessageEntry>(messageEntry.getOwner(), messageEntry);
            }
        });
        System.out.println("GIVING A KEY TO EACH ENTRY");
        System.out.println(ownerMessage.count());
        ownerMessage.foreach(new VoidFunction<Tuple2<String, MessageEntry>>() {
            public void call(Tuple2<String, MessageEntry> stringMessageEntryTuple2) throws Exception {
                System.out.println(stringMessageEntryTuple2);
            }
        });

        // count how many messages each owner has
        JavaPairRDD<String, Integer> ownerNumberOfMessages = ownerMessage.combineByKey(new Function<MessageEntry, Integer>() {
            public Integer call(MessageEntry messageEntry) throws Exception {
                return 1;
            }
        }, new Function2<Integer, MessageEntry, Integer>() {
            public Integer call(Integer integer, MessageEntry messageEntry) throws Exception {
                return integer + 1;
            }
        }, new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer;
            }
        });
        System.out.println("OWNER - NUMBER OF MESSAGES");
        System.out.println(ownerNumberOfMessages.count());
        ownerNumberOfMessages.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });


        System.out.println("\n\nMACHINE LEARNING IN SPARK");

        // first let's have some useful representation of the content of the message

        JavaPairRDD<String, MessageEntry> prepared_data = ownerMessage.mapToPair(new PairFunction<Tuple2<String, MessageEntry>, String, MessageEntry>() {
            public Tuple2<String, MessageEntry> call(Tuple2<String, MessageEntry> stringMessageEntryTuple2) throws Exception {
                String key = stringMessageEntryTuple2._1;
                MessageEntry message = stringMessageEntryTuple2._2;
                message.setBodySubjectNoStopWords(Utils.removeStopWords(message.getSubject() + message.getBody()));
                return new Tuple2<String, MessageEntry>(key, message);
            }
        });
        System.out.println("CONTENT NO STOP WORDS");
        System.out.println(prepared_data.count());
        prepared_data.foreach(new VoidFunction<Tuple2<String, MessageEntry>>() {
            public void call(Tuple2<String, MessageEntry> stringVectorTuple2) throws Exception {
                System.out.println(stringVectorTuple2);
            }
        });

        // let's produce tfidf
        JavaPairRDD<String, Tuple2<MessageEntry, Vector>> withTermFrequencies = prepared_data.mapToPair(new PairFunction<Tuple2<String, MessageEntry>, String, Tuple2<MessageEntry, Vector>>() {
            public Tuple2<String, Tuple2<MessageEntry, Vector>> call(Tuple2<String, MessageEntry> stringMessageEntryTuple2) throws Exception {
                String key = stringMessageEntryTuple2._1;
                MessageEntry message = stringMessageEntryTuple2._2;
                HashingTF hashingTF = new HashingTF();
                Vector termFrequency = hashingTF.transform(Arrays.asList(message.getBodySubjectNoStopWords().split(" ")));
                Tuple2<MessageEntry, Vector> value = new Tuple2<MessageEntry, Vector>(message, termFrequency);
                return new Tuple2<String, Tuple2<MessageEntry, Vector>>(key, value);
            }
        });

        System.out.println("\n\nWITH TERM FREQUENCIES");
        System.out.println(withTermFrequencies.count());
        withTermFrequencies.foreach(new VoidFunction<Tuple2<String, Tuple2<MessageEntry, Vector>>>() {
            public void call(Tuple2<String, Tuple2<MessageEntry, Vector>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2);
            }
        });

        // let's double

        JavaPairRDD<String, Tuple2<Vector, MessageEntry>> doubled = prepared_data.mapToPair(new PairFunction<Tuple2<String, MessageEntry>, String, Tuple2<Vector, MessageEntry>>() {
            public Tuple2<String, Tuple2<Vector, MessageEntry>> call(Tuple2<String, MessageEntry> stringMessageEntryTuple2) throws Exception {
                String key = stringMessageEntryTuple2._1;
                MessageEntry message = stringMessageEntryTuple2._2;
                message.setBodySubjectNoStopWords(message.getBodySubjectNoStopWords() + " " + message.getBodySubjectNoStopWords());
                HashingTF hashingTF = new HashingTF();
                Vector termfrequency = hashingTF.transform(Arrays.asList(message.getBodySubjectNoStopWords().split(" ")));
                return new Tuple2<String, Tuple2<Vector, MessageEntry>>(key, new Tuple2<Vector, MessageEntry>(termfrequency, message));
            }
        });

        System.out.println("\n\nDOUBLED");
        System.out.println(doubled.count());
        doubled.foreach(new VoidFunction<Tuple2<String, Tuple2<Vector, MessageEntry>>>() {
            public void call(Tuple2<String, Tuple2<Vector, MessageEntry>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2);
            }
        });

        // NORMALISATION
        // cogroup
        JavaPairRDD<String, Tuple2<Iterable<Tuple2<MessageEntry, Vector>>, Iterable<Tuple2<Vector, MessageEntry>>>> cogrouped = withTermFrequencies.cogroup(doubled);
        System.out.println("\n\nCOGROUPED");
        System.out.println(cogrouped.count());
        cogrouped.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<Tuple2<MessageEntry, Vector>>, Iterable<Tuple2<Vector, MessageEntry>>>>>() {
            public void call(Tuple2<String, Tuple2<Iterable<Tuple2<MessageEntry, Vector>>, Iterable<Tuple2<Vector, MessageEntry>>>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2);
            }
        });

        // flatten
        JavaPairRDD<Long, Tuple2<Vector, Vector>> bothVectors = cogrouped.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<Tuple2<MessageEntry, Vector>>, Iterable<Tuple2<Vector, MessageEntry>>>>, Long, Tuple2<Vector, Vector>>() {
            public Iterable<Tuple2<Long, Tuple2<Vector, Vector>>> call(Tuple2<String, Tuple2<Iterable<Tuple2<MessageEntry, Vector>>, Iterable<Tuple2<Vector, MessageEntry>>>> stringTuple2Tuple2) throws Exception {
                LinkedList<Tuple2<Long, Tuple2<Vector, Vector>>> flatten = new LinkedList<Tuple2<Long, Tuple2<Vector, Vector>>>();

                for (Tuple2<MessageEntry, Vector> mess_vec_tuple : stringTuple2Tuple2._2._1) {
                    Long message_id = mess_vec_tuple._1.getMid();
                    for (Tuple2<Vector, MessageEntry> vec_mes_tuple : stringTuple2Tuple2._2._2) {
                        if (vec_mes_tuple._2.getMid().equals(message_id)) {
                            flatten.add(new Tuple2<Long, Tuple2<Vector, Vector>>(message_id, new Tuple2<Vector, Vector>(mess_vec_tuple._2, vec_mes_tuple._1)));
                        }
                    }
                }

                return flatten;
            }
        });

        System.out.println("\n\nBOTH VECTORS");
        System.out.println(bothVectors.count());
        bothVectors.foreach(new VoidFunction<Tuple2<Long, Tuple2<Vector, Vector>>>() {
            public void call(Tuple2<Long, Tuple2<Vector, Vector>> longTuple2Tuple2) throws Exception {
                System.out.println(longTuple2Tuple2);
            }
        });

        // finally: normalise
        JavaPairRDD<Long, Tuple2<Vector, Vector>> normalised = bothVectors.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Vector, Vector>>, Long, Tuple2<Vector, Vector>>() {
            public Tuple2<Long, Tuple2<Vector, Vector>> call(Tuple2<Long, Tuple2<Vector, Vector>> longTuple2Tuple2) throws Exception {
                Long id = longTuple2Tuple2._1;
                Vector vec1 = longTuple2Tuple2._2._1;
                Vector vec2 = longTuple2Tuple2._2._2;

                Normalizer normalizer = new Normalizer(2);
                vec1 = normalizer.transform(vec1);
                vec2 = normalizer.transform(vec2);

                return new Tuple2<Long, Tuple2<Vector, Vector>>(id, new Tuple2<Vector, Vector>(vec1, vec2));
            }
        });

        System.out.println("NORMALISED");
        System.out.println(normalised.count());
        normalised.foreach(new VoidFunction<Tuple2<Long, Tuple2<Vector, Vector>>>() {
            public void call(Tuple2<Long, Tuple2<Vector, Vector>> longTuple2Tuple2) throws Exception {
                System.out.println(longTuple2Tuple2);
            }
        });

    }
}
