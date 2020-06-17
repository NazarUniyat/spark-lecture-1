package apps;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import utils.SpaceXLaunch;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class FourthBlock {
    public static void main(String[] args) {

//        String path = args[0];
        String path = "in/launches.json";
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("spaceX-app")
                .getOrCreate();

        Dataset<Row> spaceXLaunches = sparkSession
                .read()
                .json(path);

        Dataset<Row> selectedSpaceXLaunches = spaceXLaunches
                .select(
                        col("launch_year").as("launchYear").cast("integer"),
                        flatten(col("rocket.second_stage.payloads.customers")).as("customers"),
                        col("launch_success").as("launchSuccess")
                );
        selectedSpaceXLaunches.printSchema();

//      Count successful and unsuccessful launch where rocket.second_stage.payloads.customers contains “NASA”.
        Dataset<SpaceXLaunch> nasaRelatedLaunches = selectedSpaceXLaunches
                .as(Encoders.bean(SpaceXLaunch.class))
                .filter(
                        launch -> launch.getCustomers() != null &&
                                Arrays.stream(launch.getCustomers()).anyMatch(customer -> customer.contains("NASA")));
        RelationalGroupedDataset launchSuccess = nasaRelatedLaunches
                .groupBy(col("launchSuccess"));
        Dataset<Row> count = launchSuccess.count();
        count.show(10);


//      Calculate success rate for each Falcon Heavy booster landing
        Dataset<Row> falconHeavy = spaceXLaunches
                .filter(col("rocket.rocket_id").equalTo("falconheavy"))
                .select(col("rocket.first_stage.cores.land_success").as("boosterReturnSuccess"));
        falconHeavy.show(10);

        Dataset<Row> atLeastOneFailed = falconHeavy.filter(
                array_contains(col("boosterReturnSuccess"), false)
        );

        Dataset<Row> coreLandingFails = atLeastOneFailed
                .filter(element_at(col("boosterReturnSuccess"), 1).equalTo(false));
        coreLandingFails.show(10);

        Dataset<Row> side1Fails = atLeastOneFailed
                .filter(element_at(col("boosterReturnSuccess"), 2).equalTo(false));
        side1Fails.show(10);

        Dataset<Row> side2Fails = atLeastOneFailed
                .filter(element_at(col("boosterReturnSuccess"), 3).equalTo(false));
        side2Fails.show(10);

        long totalLaunches = falconHeavy.count();
        System.out.println("core success rate " + (100 - (((double) coreLandingFails.count() / (double) totalLaunches) * 100)));
        System.out.println("side 1 success rate " + (100 - (((double) side1Fails.count() / (double) totalLaunches) * 100)));
        System.out.println("side 2 success rate " + (100 - (((double) side2Fails.count() / (double) totalLaunches) * 100)));
    }
}
