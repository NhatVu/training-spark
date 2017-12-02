package buoi2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Tranformation {
	private transient JavaSparkContext context;
	private SparkConf conf = null;

	public Tranformation() {
		this.InitSpark();
	}

	private void InitSpark() {
		this.conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");

		this.context = new JavaSparkContext(conf);
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
	}

	public static void main(String[] args) {
		Tranformation tranformation = new Tranformation();
		//tranformation.demoMap();
		tranformation.demoFilter();
	}
}
