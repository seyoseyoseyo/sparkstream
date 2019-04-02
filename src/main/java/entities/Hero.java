package entities;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Hero {
    public int PlayerID;
    public int currentLevel;
    public int currentXP;
    public int currentHealth;
    public int currentLifeState;
    public int cellX;
    public int cellY;
    public float vecX;
    public float vecY;
    public int teamNumber;
    public float currentStrength;
    public float currentAgility;
    public float currentIntellect;
    public int damageMin;
    public int damageMax;

    public static Dataset<Row> generateStream(SparkSession spark) {
        Dataset<Row> data = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", 100)
                .load()
                .select(
                        "value % 10 as playerID",
                        "value % 25 as level",
                        "value % 27235 as xp",
                        "value % 3000 as health",
                        "value % 4 as lifeState",
                        "value % 128 as cellX",
                        "value % 128 as cellY",
                        "(value % 1280) / 10 as vecX",
                        "(value % 1280) / 10 as vecY",
                        "value % 1 as teamNumber",
                        "value % 200 as strength",
                        "value % 200 as agility",
                        "value % 200 as intellect",
                        "value % 100 as damageMin",
                        "value % 100 as damageMax"
                );
        return data;
    }
}
