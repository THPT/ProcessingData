import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ProcessingData {
	static Connection connection;

	private static Connection createConnection() throws Exception {
		return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/grok?user=grok&password=grok&ssl=false");
	}

	public static void extractCategory(SparkSession spark) {
		System.out.println("ExtractCategory...");
		Dataset<Row> categories = spark.sql("SELECT DISTINCT category FROM product order by category");
		try {
			connection.createStatement();
			List<Row> rows = categories.collectAsList();
			for (Row row : rows) {
				String query = "INSERT INTO categories(category_name) VALUES(?)";
				PreparedStatement stm = connection.prepareStatement(query);
				stm.setString(1, row.getString(0));
				stm.execute();
			}

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
