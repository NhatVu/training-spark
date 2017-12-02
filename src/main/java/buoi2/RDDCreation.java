package buoi2;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class RDDCreation {
	private transient JavaSparkContext context;
	private SparkConf conf = null;

	
	public RDDCreation() {
		this.InitSpark();
	}

	private void InitSpark() {
		this.conf = new SparkConf().setAppName("Log Analysis")
		.setMaster("local[*]");

		this.context = new JavaSparkContext(conf);
	};
	
	void createRDDParallel() {
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> rdd = context.parallelize(data);
		
		List<Integer> result = rdd.collect();
		for(Integer tmp : result) {
			System.out.println(tmp);
		}
	}
	
	void createRDDExternalSource() {
		JavaRDD<String> rdd = context.textFile("data/people.txt");
		
		List<String> result = rdd.collect();
		for(String tmp : result) {
			System.out.println(tmp);
		}
	}
	
	
	public static void main(String[] args) {
		RDDCreation creation = new RDDCreation();
		//creation.createRDDParallel();
		creation.createRDDExternalSource();
	}
}
