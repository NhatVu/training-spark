package buoi2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Tranformation {
	private transient JavaSparkContext context;
	private SparkConf conf = null;
	private SparkSession session;

	public Tranformation() {
		this.InitSpark();
	}

	private void InitSpark() {
		this.conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");

		this.context = new JavaSparkContext(conf);
		this.session = new SparkSession.Builder()
				// .config(this.MHCConf)
				.config("spark.sql.warehouse.dir", "warehouseLocation").getOrCreate();
	};

	public void demoMap() {
		JavaRDD<String> data = context.textFile("data/number.txt");

		JavaRDD<Integer> number = data.map(x -> Integer.parseInt(x));

		for (Integer tmp : number.collect()) {
			System.out.println(tmp);
		}

	}

	public void demoFilter() {
		JavaRDD<String> data = context.textFile("data/number.txt");

		JavaRDD<Integer> number = data.map(x -> Integer.parseInt(x));

		// chỉ dữ lại các số > 3
		number = number.filter(x -> x > 3);
		
		for (Integer tmp : number.collect()) {
			System.out.println(tmp);
		}
		
		Dataset<Row> df = session.createDataFrame(number, Integer.class);
		df.write().parquet("data/parquet");
	}

	public static void main(String[] args) {
		Tranformation tranformation = new Tranformation();
		//tranformation.demoMap();
		tranformation.demoFilter();
	}
}
