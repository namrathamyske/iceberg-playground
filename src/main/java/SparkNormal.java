import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class SparkNormal {
    private static String SAMPLE_TABLE_NAME = "exp";

    public SparkNormal() {
    }

    public static void main(String[] args) throws TableAlreadyExistsException, InterruptedException {
        SparkConf sparkConf = getSparkConfig();
        SparkSession spark = SparkSession.builder().appName("Spark SQL basic example").config(sparkConf).master("local").getOrCreate();
        Dataset<Row> df = spark.read().option("header", "true").option("inferSchema", "true").load("/Users/nkeshavaprakash/Documents/cdp-repos/iceberg-playground/spark-warehouse/covid");
        Column column = df.col("cord_uid");
        long startTime = System.currentTimeMillis();
        System.out.println(df.dropDuplicates("cord_uid", new String[0]).count());
        Dataset<Row> df1 = df.filter("cord_uid == '0ourzdtt'");
        long stopTime = System.currentTimeMillis();
        System.out.println("Time taken :");
        System.out.println(stopTime - startTime);
        df1.show();
        System.out.println("Hello");
        Thread.sleep(240000L);
    }

    @Test
    public void testQueryViews() throws NoSuchTableException, ParseException {
        SparkConf config = getSparkConfig();
        SparkSession spark = SparkSession.builder().appName("icebergTesting").master("local").config(config).getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        StructType schema = DataTypes.createStructType(new StructField[]{DataTypes.createStructField("cord_uid", DataTypes.StringType, true), DataTypes.createStructField("sha", DataTypes.StringType, true), DataTypes.createStructField("source_x", DataTypes.StringType, true), DataTypes.createStructField("title", DataTypes.StringType, true), DataTypes.createStructField("doi", DataTypes.StringType, true), DataTypes.createStructField("pmcid", DataTypes.StringType, true), DataTypes.createStructField("pubmed_id", DataTypes.StringType, true), DataTypes.createStructField("license", DataTypes.StringType, true), DataTypes.createStructField("abstract", DataTypes.StringType, true), DataTypes.createStructField("publish_time", DataTypes.StringType, true)});
        String path = "/Users/nkeshavaprakash/Documents/cdp-repos/iceberg-playground/src/main/resources/covid_metadata-102.csv";
        Dataset<Row> df = spark.read().format("iceberg").option("header", "true").option("inferSchema", "true").schema(schema).csv(path);
        df.writeTo(SAMPLE_TABLE_NAME).create();
        path = "/Users/nkeshavaprakash/Documents/cdp-repos/iceberg-playground/src/main/resources/covid_metadata-103.csv";
        Dataset<Row> df1 = spark.read().option("header", "true").option("inferSchema", "true").schema(schema).csv(path);
        df1.writeTo(SAMPLE_TABLE_NAME).append();
        path = "/Users/nkeshavaprakash/Documents/cdp-repos/iceberg-playground/src/main/resources/covid_metadata-104.csv";
        Dataset<Row> df2 = spark.read().format("iceberg").option("header", "true").option("inferSchema", "true").schema(schema).csv(path);
        df2.writeTo(SAMPLE_TABLE_NAME).append();
        System.out.println("Snapshot-3:");
        Dataset<Row> lastdata = spark.read().table(SAMPLE_TABLE_NAME);
        lastdata.show();
        System.out.println("Snapshot-2: This is when we add ref to the table");
        String viewSql = String.format("CREATE VIEW permanent_view AS SELECT * FROM %s", SAMPLE_TABLE_NAME);
        spark.sql(viewSql);
        spark.sql("SHOW VIEWS").show();
        Dataset<Row> tble = spark.table("permanent_view");
        tble.show();
    }

    private static SparkConf getSparkConfig() {
        SparkConf config = new SparkConf();
        config.set("spark.sql.legacy.createHiveTableByDefault", "false");
        config.set("spark.sql.catalog.local.warehouse", "spark-warehouse-exp");
        return config;
    }
}
