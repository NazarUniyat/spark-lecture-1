package apps;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;


public class FirstBlock {
    public static void main(String[] args) {

//        String path = args[0];
        String path = "in/launches.json";
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("spaceX-app")
                .getOrCreate();

        Dataset<Row> spaceXHistory = sparkSession
                .read()
                .json(path);
        spaceXHistory.printSchema();
        spaceXHistory.show(100);


        Dataset<Row> spaceXLaunchesSelected =
                spaceXHistory
                        .select(
                                col("launch_year"),
                                col("launch_success"),
                                col("rocket.rocket_name"))
                        .orderBy(col("launch_year"));
        spaceXLaunchesSelected.show(100);

    }
}
