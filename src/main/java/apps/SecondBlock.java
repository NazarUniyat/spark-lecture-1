package apps;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import utils.EnrichedLaunch;
import utils.Launch;

import static org.apache.spark.sql.functions.col;

public class SecondBlock {
    public static void main(String[] args) {

//      String path = args[0];
        String path = "in/launches.json";
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("spaceX-app")
                .getOrCreate();

        Dataset<Row> spaceXHistory = sparkSession
                .read()
                .json(path);

//      Select as Dataset<Row> and Dataset<Typed>
        Dataset<Row> spaceXLaunchesSelected = spaceXHistory
                .select(
                        col("launch_year").as("launchYear").cast("integer"),
                        col("mission_name").as("missionName"),
                        col("launch_success").as("launchSuccess"),
                        col("details"));
        spaceXLaunchesSelected.printSchema();
        Dataset<Launch> launches = spaceXLaunchesSelected.as(Encoders.bean(Launch.class));

//      Filter records where mission name contains "starlink" for <Typed> and <Row> Datasets
        spaceXLaunchesSelected.filter((FilterFunction<Row>) row -> row.<String>getAs("missionName").toLowerCase().contains("starlink")).show();
        launches.filter((FilterFunction<Launch>) launch -> launch.getMissionName().toLowerCase().contains("starlink")).show();

//      Map existing Dataset<Typed> and Dataset<Row> to enriched Dataset
        Dataset<EnrichedLaunch> enrichedLaunches = launches.map((MapFunction<Launch, EnrichedLaunch>) value ->
                        EnrichedLaunch.of(value, "wow, spaceX is so cool"),
                Encoders.bean(EnrichedLaunch.class));
        enrichedLaunches.show(20);


        StructType schema = spaceXLaunchesSelected.schema();
        schema = schema.add("veryImportantComment", DataTypes.StringType);

        Dataset<Row> mapped = spaceXLaunchesSelected.map((MapFunction<Row, Row>) row -> RowFactory.create(
                row.<Integer>getAs("launchYear"),
                row.<String>getAs("missionName"),
                row.<String>getAs("launchSuccess"),
                row.<String>getAs("details"),
                "WOW, spaceX is so cool"), RowEncoder.apply(schema)
        );
        mapped.show(10);


    }
}
