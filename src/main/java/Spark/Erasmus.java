package Spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.hadoop.shaded.com.google.common.collect.Multisets.filter;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class Erasmus {
    public static void main(String[] csv){
        SparkSession sparkSession = SparkSession.builder().master("local").appName("ReadCsv").getOrCreate();
        String filePath="src/main/resources/Erasmus.csv";
        Dataset<Row> dataset = sparkSession.sqlContext().read().format("com.databricks.spark.csv")
                .option("header",true)
                .load(filePath);
        dataset.show(5,false);

        Dataset<Row> filteredDF = dataset.groupBy("Receiving Country Code","Sending Country Code")
                .count().alias("count").sort("Receiving Country Code");
        filteredDF.show(50);
        //filteredDF.write().format("csv").save("src/main/resources/FilteredData");
        sparkSession.close();
    }
}
