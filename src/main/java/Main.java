import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {



    SparkSession spark = SparkSession
            .builder()
            .appName("SparkRateTest")
            .getOrCreate();

    public void readStream() {
        Dataset<Row> data = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", 100)
                .load();

    }

    public static void main(String[] args) {

    }
}
