import com.google.common.collect.Iterables;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.IcebergSpark;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IcebergTableTestBucketingCoviddata {
    private static String SAMPLE_TABLE_NAME = "local.db.table";

    public IcebergTableTestBucketingCoviddata() {
    }

    @Test
    public void testInsertDataWithBucketPartitions() throws NoSuchTableException {
        SparkConf config = getSparkConfig();
        SparkSession spark = SparkSession.builder().appName("icebergTesting").master("local").config(config).getOrCreate();
        StructType schema = DataTypes.createStructType(new StructField[]{DataTypes.createStructField("cord_uid", DataTypes.StringType, true), DataTypes.createStructField("sha", DataTypes.StringType, true), DataTypes.createStructField("source_x", DataTypes.StringType, true), DataTypes.createStructField("title", DataTypes.StringType, true), DataTypes.createStructField("doi", DataTypes.StringType, true), DataTypes.createStructField("pmcid", DataTypes.StringType, true), DataTypes.createStructField("pubmed_id", DataTypes.StringType, true), DataTypes.createStructField("license", DataTypes.StringType, true), DataTypes.createStructField("abstract", DataTypes.StringType, true), DataTypes.createStructField("publish_time", DataTypes.StringType, true)});
        String path = "/Users/nkeshavaprakash/Documents/cdp-repos/iceberg-playground-test/iceberg-playground/src/main/resources/covid_metadata.csv";
        Dataset<Row> df = spark.read().format("iceberg").option("header", "true").option("inferSchema", "true").schema(schema).csv(path);
        IcebergSpark.registerBucketUDF(spark, "iceberg_bucket16", DataTypes.StringType, 16);
        Column column = df.col("cord_uid");
        df.sortWithinPartitions(new Column[]{functions.expr("iceberg_bucket16(cord_uid)")}).writeTo(SAMPLE_TABLE_NAME).partitionedBy(functions.bucket(16, column), new Column[0]).createOrReplace();
        df.show();
        df = spark.read().format("iceberg").load(SAMPLE_TABLE_NAME);
        df.show();
    }

    @Test
    public void testSnapShot() {
        SparkConf config = getSparkConfig();
        SparkSession spark = SparkSession.builder().appName("icebergTesting").master("local").config(config).getOrCreate();
        String getHistory = String.format("SELECT * FROM %s", SAMPLE_TABLE_NAME + ".history");
        spark.sql(getHistory).show();
        String getSnapshots = String.format("SELECT * FROM %s", SAMPLE_TABLE_NAME + ".snapshots");
        spark.sql(getSnapshots).show();
        String getManifests = String.format("SELECT * FROM %s", SAMPLE_TABLE_NAME + ".manifests");
        spark.sql(getManifests).show();
        Dataset<Row> finalData = spark.read().option("snapshot-id", "4923701791080026097").format("iceberg").load("/Users/nkeshavaprakash/Documents/cdp-repos/iceberg-playground/iceberg-playground-test/spark-warehouse-covid/db/table");
        finalData.show();
    }

    @Test
    public void testExpiry() throws InterruptedException, NoSuchTableException, ParseException {
        SparkConf config = getSparkConfig();
        SparkSession spark = SparkSession.builder().appName("icebergTesting").master("local").config(config).getOrCreate();
        Table table = Spark3Util.loadIcebergTable(spark, "local.db.table");
        System.out.println("The total number of snapshots are:");
        System.out.println(Iterables.size(table.snapshots()));
        String selectSparkSql = String.format("SELECT * from %s WHERE %s = %s", SAMPLE_TABLE_NAME, "local.db.table.cord_uid", "'wnnsmx60'");
        Dataset<Row> df1 = spark.sql(selectSparkSql);
        df1.show();
        String sparkSql = String.format("DELETE from %s WHERE %s = %s", SAMPLE_TABLE_NAME, "local.db.table.cord_uid", "'wnnsmx60'");
        Dataset<Row> df = spark.sql(sparkSql);
        df.show();
        table.expireSnapshots().expireOlderThan(table.currentSnapshot().snapshotId()).commit();
        df1 = spark.sql(selectSparkSql);
        df1.show();
        System.out.println("The total number of snapshots are:");
        System.out.println(Iterables.size(table.snapshots()));
    }

    @Test
    public void testBranch() throws InterruptedException, NoSuchTableException, ParseException {
        SparkConf config = getSparkConfig();
        SparkSession spark = SparkSession.builder().appName("icebergTesting").master("local").config(config).getOrCreate();
        Table table = Spark3Util.loadIcebergTable(spark, "local.db.table");
        System.out.println("The total number of snapshots are:");
        System.out.println(Iterables.size(table.snapshots()));
        String selectSparkSql = String.format("SELECT * from %s WHERE %s = %s", SAMPLE_TABLE_NAME, "local.db.table.cord_uid", "'wnnsmx60'");
        Dataset<Row> df1 = spark.sql(selectSparkSql);
        df1.show();
        String sparkSql = String.format("DELETE from %s WHERE %s = %s", SAMPLE_TABLE_NAME, "local.db.table.cord_uid", "'wnnsmx60'");
        Dataset<Row> df = spark.sql(sparkSql);
        df.show();
        table.expireSnapshots().expireOlderThan(table.currentSnapshot().snapshotId()).commit();
        df1 = spark.sql(selectSparkSql);
        df1.show();
        System.out.println("The total number of snapshots are:");
        System.out.println(Iterables.size(table.snapshots()));
    }

    @Test
    public void testSelect() throws InterruptedException, NoSuchTableException, ParseException {
        SparkConf config = getSparkConfig();
        SparkSession spark = SparkSession.builder().appName("icebergTesting").master("local").config(config).getOrCreate();
        long startTime = System.currentTimeMillis();
        String sparkSql = String.format("SELECT * from %s WHERE %s = %s", SAMPLE_TABLE_NAME, "local.db.table.cord_uid", "'ejv2xln0'");
        Dataset<Row> df = spark.sql(sparkSql);
        long stopTime = System.currentTimeMillis();
        System.out.println("Time taken :");
        System.out.println(stopTime - startTime);
        df.show();
        System.out.println("Hello");
        df.explain();
        Thread.sleep(240000L);
    }

    @Test
    public void update() throws InterruptedException {
        SparkConf config = getSparkConfig();
        SparkSession spark = SparkSession.builder().appName("icebergTesting").master("local").config(config).getOrCreate();
        long startTime = System.currentTimeMillis();
        String sparkSql = String.format("UPDATE * SET license=%s WHERE %s = %s", SAMPLE_TABLE_NAME, "local.db.table.cord_uid", "'0ourzdtt'");
        Dataset<Row> df = spark.sql(sparkSql);
        long stopTime = System.currentTimeMillis();
        System.out.println("Time taken :");
        System.out.println(stopTime - startTime);
        df.show();
        System.out.println("Hello");
        Thread.sleep(5000L);
    }

    private static SparkConf getSparkConfig() {
        SparkConf config = new SparkConf();
        config.set("spark.sql.legacy.createHiveTableByDefault", "false");
        config.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        config.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
        config.set("spark.sql.catalog.spark_catalog.type", "hive");
        config.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog");
        config.set("spark.sql.catalog.local.type", "hadoop");
        config.set("spark.sql.catalog.local.warehouse", "spark-warehouse-covid");
        return config;
    }
}
