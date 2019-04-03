import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.SQLContext.implicits$;

import static org.apache.arrow.flatbuf.Type.Map;

public class Main {

    public static Dataset<Row> generateStream(SparkSession spark) {
        Dataset<Row> data = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", 100)
                .load()
                .selectExpr(
                        "value % 10 as playerID",
                        "value % 25 as level",
                        "value % 27235 as xp",
                        "value % 3000 as health",
                        "value % 4 as lifeState",
                        "value % 128 as cellX",
                        "value % 128 as cellY",
                        "(value % 1280) / 10 as vecX",
                        "(value % 1280) / 10 as vecY",
                        "value % 2 as teamNumber",
                        "value % 200 as strength",
                        "value % 200 as agility",
                        "value % 200 as intellect",
                        "value % 100 as damageMin",
                        "value % 100 as damageMax",
                        "timestamp"
                )
        ;
        return data;
    }

    public static Dataset<Row> basicSelection(Dataset<Row> data) {
        return data.filter("teamNumber = 1");
    }

    public static Dataset<Row> statefulAggregation(Dataset<Row> data) {
        Dataset<Row> max = data.groupBy("playerID").count();
        return max;
    }

    public static Dataset<Row> windowedAverage(Dataset<Row> data) {
        Dataset<Row> windowedCounts = data
                //.withWatermark("timestamp", "10 seconds")
                .groupBy(
                        functions.window(data.col("timestamp"), "10 seconds", "10 seconds"),
                        data.col("playerID")
                ).max("level")
                .orderBy("window");

        return windowedCounts;
    }

    public static Dataset<Row> leaderboard(Dataset<Row> data) {
        Dataset<Row> leaderboard = data
                .groupBy("playerID")
                .max("level");
        return leaderboard;
    }

    public static StreamingQuery generateQuery(Dataset<Row> data) {
        StreamingQuery query = data
                .writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .option("numRows", 100)
                //.trigger(Trigger.ProcessingTime("5 minutes"))
                .start();
        return query;
    }




    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkRateTest")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> streamData = generateStream(spark);
        Dataset<Row> team1 = leaderboard(streamData);
        StreamingQuery query  = generateQuery(team1);

        query.awaitTermination();
    }
}
