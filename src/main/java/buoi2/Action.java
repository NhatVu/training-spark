package buoi2;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Action {
	private transient JavaSparkContext context;
	private SparkConf conf = null;

	public Action() {
		this.initSpark();
	}

	private void initSpark() {
		this.conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");

		this.context = new JavaSparkContext(conf);
	};
	
	private void commonAction() {
		JavaRDD<String> data = context.textFile("data/number.txt");
		// đếm số phần tử trong data RDD 
		System.out.println("số dòng là: " + data.count());
		
		// lấy phần tử đầu tiên 
		System.out.println("phần tử đầu tiên: " + data.first());
		
		// thu thập RDD thành list 
		List<String> result = data.collect();
		for(String tmp : result) {
			System.out.println(tmp);
		}
		
	}
	
	public static void main(String[] args) {
		Action action = new Action();
		action.commonAction();
	}
}
