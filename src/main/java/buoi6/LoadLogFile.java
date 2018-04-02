package buoi6;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Function1;

public class LoadLogFile {
    private SparkConf sparkConf;
    private transient JavaSparkContext sparkContext;
    private SparkSession sparkSession;

    public LoadLogFile(){
        initSpark();
    }
    private void initSpark() {
        this.sparkConf = new SparkConf().setAppName("Load log file")
                        .setMaster("local");
        String warehouseLocation = "spark-warehouse";

        this.sparkContext = new JavaSparkContext(sparkConf);

        this.sparkSession = new SparkSession.Builder().config(this.sparkConf)
                .config("spark.sql.warehouse.dir", warehouseLocation).getOrCreate();
    };
	private static List<StructField> field = Arrays.asList(
			DataTypes.createStructField("userID", DataTypes.LongType, false),
			DataTypes.createStructField("domain", DataTypes.StringType, true),
			DataTypes.createStructField("url", DataTypes.StringType, true));
	public static StructType schema = DataTypes.createStructType(field);
	public static ExpressionEncoder<Row> Encoder = RowEncoder.apply(schema);
	
    public void loadFile(){
        Dataset<String> logDFString= this.sparkSession.read().textFile("data/logFile.dat");
        System.out.println(logDFString.count());

        Dataset<Row> logDF = logDFString.map( string -> {
            String[] split = string.split(",");
            return RowFactory.create( Long.parseLong(split[2]),split[1], split[3]);
        },Encoder);
        
        logDF.show();
    }

    public static void main(String[] args) {
        LoadLogFile loadLogFile = new LoadLogFile();
        loadLogFile.loadFile();
        System.out.println("Done");
    }
}
