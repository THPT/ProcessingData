import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ProcessingData {
	public static class Category implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		String categoryName;
		public String getCategoryName() {
			return categoryName;
		}

		public void setCategoryName(String categoryName) {
			this.categoryName = categoryName;
		}
	}

	static Connection connection;

	private static Connection createConnection() throws Exception {
		return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/grok?user=grok&password=grok&ssl=false");
	}


	public static void extractCategory(SparkSession spark) {
		Dataset<Row> categories = spark.sql("SELECT DISTINCT category as categoryName FROM product");
		try {
			connection.createStatement();
			Encoder<Category> categoryEncoder = Encoders.bean(Category.class);
			Dataset<Category> categoryDF = categories.as(categoryEncoder);
			categoryDF.foreach(new ForeachFunction<Category>() {
				
				@Override
				public void call(Category t) throws Exception {
					String query = "INSERT INTO categories(category_name) VALUES(?)";
					PreparedStatement stm = connection.prepareStatement(query);
					stm.setString(1, t.categoryName);
					stm.execute();
				}
			});
//			List<Row> rows = categories.collectAsList();
//			for (Row row : rows) {
//				System.out.println("PROCESSING...");
//				String query = "INSERT INTO categories(category_name) VALUES(?)";
//				PreparedStatement stm = connection.prepareStatement(query);
//				stm.setString(1, row.getString(0));
//				stm.execute();
//			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			connection = createConnection();
			connection.setAutoCommit(false);
		} catch (Exception e) {
			e.printStackTrace();
		}

		SparkSession spark = SparkSession.builder().appName("ProcessingData")
				.config("spark.sql.warehouse.dir", "/user/hive/warehouse").enableHiveSupport().getOrCreate();
		spark.sql("show tables").show();
		extractCategory(spark);

	}
}
