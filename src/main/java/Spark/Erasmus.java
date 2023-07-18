package Spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class Erasmus {
    public static void main(String[] csv){
        SparkSession sparkSession = SparkSession.builder().master("local").appName("ReadCsv").getOrCreate();
        String filePath="src/main/resources/Erasmus.csv";
        Dataset<Row> dataset = sparkSession.sqlContext().read().format("com.databricks.spark.csv")
                .option("header",true)
                .load(filePath);
        dataset.show(5,false);

        Dataset<Row> filteredDF = dataset.filter(col("Receiving Country Code").isin("LV","MK","MT"))
                .groupBy("Receiving Country Code","Sending Country Code")
                .count()
                .alias("count")
                .sort("Receiving Country Code","Sending Country Code");
        filteredDF.show(50);

        filteredDF.write()
                .format("jdbc")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/spark?serverTimezone=Europe/Chisinau")
                .option("dbtable", "rcc")
                .option("user", "root")
                .option("password", "root")
                .mode(SaveMode.Overwrite)
                .save();

        sparkSession.close();
    }
}
