package buoi7;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SparkSQL {
	private SparkConf sparkConf;
	private transient JavaSparkContext sparkContext;
	private SparkSession sparkSession;
	private Dataset<Row> uci;

	public SparkSQL() {
		initSpark();
		loadData();
	}

	private void initSpark() {
		this.sparkConf = new SparkConf().setAppName("Load log file").setMaster("local");
		String warehouseLocation = "spark-warehouse";

		this.sparkContext = new JavaSparkContext(sparkConf);

		this.sparkSession = new SparkSession.Builder().config(this.sparkConf)
				.config("spark.sql.warehouse.dir", warehouseLocation).getOrCreate();
	};
	
	private void loadData() {
		uci = sparkSession.read().parquet("data/userCurrentInterest");
		uci.createOrReplaceTempView("uci");
		System.out.println("uci show");
		uci.show();
		/*
 		+----------+-----------+------+
		|categoryid|probability|userid|
		+----------+-----------+------+
		|         3|        0.5|     3|
		|         4|        0.2|     3|
		|         1|       0.15|     4|
		|         2|        0.5|     4|
		|         3|       0.05|     4|
		|         4|        0.3|     4|
		|         1|        0.1|     5|
		|         2|        0.6|     5|
		|         3|       0.15|     5|
		 * */
	}
	
	//chỉ lấy cột categoryid 
	public void projectition() {
		// Dataset API 
		System.out.println("Dataset API");
		uci.select("categoryid").show();
		
		// Nếu dùng câu lệnh SQL 
		System.out.println("SQL query");
		sparkSession.sql("select categoryid from uci").show();
	}
	
	/**
	 * Chỉ lấy những dòng có userid > 3
	 */
	public void filtering() {
		System.out.println("Dataset API");
		uci.where("userid > 3").show();
		uci.where(uci.col("userid").$greater(3)).show();
		
		uci.filter(uci.col("userid").$greater(3)).show();
		uci.filter("userid > 3").show();
		
		System.out.println("SQL query");
		sparkSession.sql("select * from uci where userid > 3").show();
	}
	
	/**
	 * Có bao nhiêu user?
	 */
	public void distinct_userid() {
		// Dataset API 
		System.out.println(uci.select("userid").distinct().count());
		
		// SQL
		System.out.println(sparkSession.sql("select distinct(userid) from uci").count());
	}
	
	/**
	 * Với mỗi userid, tổng cột probability là bao nhiêu ?  
	 */
	public void agg_sum_probability() {
		// Dataset API 
		uci.groupBy("userid").agg(functions.sum("probability")).show();
		
		// SQL 
		sparkSession.sql("select userid, sum(probability) from uci group by userid").show();
	}
	
	/**
	 * Với mỗi userid, trung bình cột probability là bao nhiêu?
	 */
	public void agg_avg_probability() {
		// Dataset API 
		uci.groupBy("userid").agg(functions.avg("probability")).show();
		
		// SQL 
		sparkSession.sql("select userid, avg(probability) from uci group by userid").show();
	}
	
	/**
	 * Muốn tạo ra kiểu dữ liệu (userid, listCategoryID) 
	 * Tức userid và danh sách categoryid
	 */
	public void agg_collect_list() {
		// Dataset API 
		uci.groupBy("userid").agg(functions.collect_list("categoryid").alias("listCategoryID")).show();
		
		// SQL 
		sparkSession.sql("select userid, collect_list(categoryid) as listCategoryID from uci group by userid").show();
	}
	
	/**
	 * Join dbnews và userCurrentInterest theo thuộc tính categoryid 
	 */
	public void join() {
		// dbnews(id,catid,url,source)
		Dataset<Row> dbnews = sparkSession.read().option("header", "true")
				.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
				.csv("data/dbnews/*");
		// uci(userid,categoryid,probability)
		Dataset<Row> afterJoin = dbnews.join(uci, dbnews.col("catid").equalTo(uci.col("categoryid")));
		afterJoin.show(100);
		
		// projectition: Lấy cột userid, categoryid, probability của uci 
		// và id (đổi thành newsid) của dbnews
		// shema có các cột sau: userid, categoryid, newsid, probability
	}
	
	public static void main(String[] args) {
		SparkSQL sql = new SparkSQL();
		//sql.projectition();
		//sql.filtering();
		//sql.distinct_userid();
		//sql.agg_sum_probability();
		//sql.agg_avg_probability();
		//sql.agg_collect_list();
		sql.join();
		System.out.println("Done");
	}
	
}
