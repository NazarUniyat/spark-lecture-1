package apps;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import utils.LaunchCSV;
import utils.SchemaUtils;

public class ThirdBlock {
    public static void main(String[] args) {

//      String path = args[0];
        String path = "in/name_year.csv";
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("spaceX-app")
                .getOrCreate();

        StructType schema = new StructType();
        schema = schema.add("mission_name", DataTypes.StringType);
        schema = schema.add("mission_year", DataTypes.IntegerType);

        Dataset<LaunchCSV> spaceXLaunches = sparkSession
                .read()
                .option("header", "true")
                .option("mode", "FAILFAST")
                .schema(schema)
                .csv(path)
                .as(Encoders.bean(LaunchCSV.class));
        spaceXLaunches.printSchema();
        spaceXLaunches.show(10);

//      an efficient way to work with schema and FAILFAST in java
        Dataset<LaunchCSV> spaceXLaunchesUsingEfficientSchemaApproach = sparkSession
                .read()
                .option("header", "true")
                .option("mode", "FAILFAST")
                .schema(SchemaUtils.toSchema(LaunchCSV.class))
                .csv(path)
                .as(Encoders.bean(LaunchCSV.class));
        spaceXLaunchesUsingEfficientSchemaApproach.printSchema();
        spaceXLaunchesUsingEfficientSchemaApproach.show(10);


    }
}
