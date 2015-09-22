import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by A046509 on 2015-09-21.
 */
public class TestMessageEntry implements Serializable {
    private static int N;

    private static JavaRDD<Integer> rdd = null;
    private static IDFModel idfModelA = null;
    private static IDFModel idfModelB = null;

    @BeforeClass
    public static void prepareData() throws IOException {
        // Initialize values
        if (TestSuite.sc == null) {
            TestSuite.prepareData();
        }
        N = 10;

        // Initialize RDD
        List<Integer> list = new LinkedList<Integer>();
        for (int i = 0; i < N; ++i)
            list.add(1);
        //list.add(2);
        rdd = TestSuite.sc.parallelize(list);
        rdd.cache();

        // Initialize IDF models
        LinkedList< Vector > corpusTMP = new LinkedList<Vector>();
        corpusTMP.add(termFrequency("this is example text for IDF model"));
        JavaRDD<Vector> corpus = TestSuite.sc.parallelize(corpusTMP);
        idfModelA = new IDF().fit(corpus);
        idfModelB = new IDF().fit(corpus);
    }

    @AfterClass
    public static void cleanData() {
        rdd.unpersist();
    }

    private static Vector termFrequency(String text) {
        // lower case, normalise white chars, trim, split with ' ', change to list
        LinkedList<String> textAsList = new LinkedList<String>(Arrays.asList(text.toLowerCase().replaceAll("\\s+", " ").trim().split(" ")));
        HashingTF hashingTF = new HashingTF();
        return hashingTF.transform(textAsList);
    }

    @Test
    public void testRddA() throws Exception {
        // Testing rdd values A
        List<Integer> list = rdd.collect();
        for (int i = 0; i < list.size(); ++i)
            assertTrue(list.get(i).equals(1));
    }

    @Test
    public void testRddB() throws Exception {
        // Testing rdd values A
        rdd.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                assertTrue(integer.equals(1));
            }
        });
    }

    @Ignore
    @Test
    public void testIDFModel() throws Exception {
        assertTrue(idfModelA.equals(idfModelA));
        assertTrue(idfModelB.equals(idfModelB));
        assertTrue(idfModelA.equals(idfModelB));
    }

    @Ignore
    @Test
    public void testIDFModelBehaviour() throws Exception {
        assertTrue(idfModelA.transform(termFrequency("IDF test")).equals(idfModelB.transform(termFrequency("IDF test"))));
        assertFalse(idfModelA.transform(termFrequency("IDF test")).equals(idfModelB.transform(termFrequency("IDF"))));
        assertFalse(idfModelA.transform(termFrequency("IDF test")).equals(idfModelB.transform(termFrequency("IDF test temp"))));
        assertFalse(idfModelA.transform(termFrequency("IDF test")).equals(idfModelB.transform(termFrequency("IDF temp"))));
        assertFalse(idfModelA.transform(termFrequency("IDF test")).equals(idfModelB.transform(termFrequency("IDF text"))));
        assertFalse(idfModelA.transform(termFrequency("IDF test")).equals(idfModelB.transform(termFrequency(""))));
    }
}
