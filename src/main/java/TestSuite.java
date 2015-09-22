import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.IOException;

/**
 * Created by A046509 on 2015-09-21.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    TestMessageEntry.class
})
public class TestSuite {
    public static JavaSparkContext sc = null;

    @BeforeClass
    public static void prepareData() throws IOException {
        SparkConf conf = new SparkConf().setAppName("LeakDetection").setMaster("local");
        sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
    }

    @AfterClass
    public static void cleanData() {
        // cleaning data be here
    }
}
