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
	static Connection connection;

	private static Connection createConnection() throws Exception {
		return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/grok?user=grok&password=grok&ssl=false");
	}

	public static void importUserStatistic(SparkSession spark) {
		Dataset<Row> orderStatisticDs = spark
				.sql("SELECT unix_timestamp(order_date) as orderDate, customer_id as userId, sum(revenue*(1-discount)) as spend "
						+ "FROM selling_order GROUP BY order_date, customer_id ORDER BY orderDate");
		try {
			connection.createStatement();
			Encoder<UserStatistic> userStatisticEncoder = Encoders.bean(UserStatistic.class);
			Dataset<UserStatistic> userStatisticDF = orderStatisticDs.as(userStatisticEncoder);
			userStatisticDF.foreach(new ForeachFunction<UserStatistic>() {

				@Override
				public void call(UserStatistic userStatistic) throws Exception {
					String query = String.format("INSERT INTO user_statistics(%s) VALUES(%s)",
							userStatistic.getColumns(), userStatistic.getArgs());
					PreparedStatement stm = connection.prepareStatement(query);
					int i = 0;
					for (Object object : userStatistic.getParameters()) {
						stm.setObject(++i, object);
					}
					stm.execute();
				}
			});
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void importOrderStatistic(SparkSession spark) {
		Dataset<Row> orderStatisticDs = spark
				.sql("SELECT unix_timestamp(order_date) as orderDate, COUNT(order_id) as totalOrder,  product.store_id as storeId, sum(revenue) as revenue "
						+ "FROM selling_order JOIN product ON product.product_id = selling_order.product_id "
						+ "GROUP BY order_date, product.store_id ORDER BY orderDate");
		try {
			connection.createStatement();
			Encoder<OrderStatistic> orderStatisticEncoder = Encoders.bean(OrderStatistic.class);
			Dataset<OrderStatistic> orderStatisticDF = orderStatisticDs.as(orderStatisticEncoder);
			orderStatisticDF.foreach(new ForeachFunction<OrderStatistic>() {

				@Override
				public void call(OrderStatistic user) throws Exception {
					String query = String.format("INSERT INTO order_statistics(%s) VALUES(%s)", user.getColumns(),
							user.getArgs());
					PreparedStatement stm = connection.prepareStatement(query);
					int i = 0;
					for (Object object : user.getParameters()) {
						stm.setObject(++i, object);
					}
					stm.execute();
				}
			});
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void importProductAge(SparkSession spark) {
		Dataset<Row> userDs = spark.sql("SELECT selling_order.product_id as productId, `user`.age "
				+ "FROM selling_order JOIN `user` ON `user`.user_id = selling_order.customer_id");
		try {
			connection.createStatement();
			Encoder<ProductAge> productAgeEncoder = Encoders.bean(ProductAge.class);
			Dataset<ProductAge> productAgeDF = userDs.as(productAgeEncoder);
			productAgeDF.foreach(new ForeachFunction<ProductAge>() {

				@Override
				public void call(ProductAge productAge) throws Exception {
					String query = String.format("INSERT INTO product_ages(%s) VALUES(%s)", productAge.getColumns(),
							productAge.getArgs());
					PreparedStatement stm = connection.prepareStatement(query);
					int i = 0;
					for (Object object : productAge.getParameters()) {
						stm.setObject(++i, object);
					}
					stm.execute();
				}
			});
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void importUser(SparkSession spark) {
		Dataset<Row> userDs = spark
				.sql("SELECT user_id as id, age, gender, unix_timestamp(creation_date) as createdAt, location FROM user");
		try {
			connection.createStatement();
			Encoder<User> userEncoder = Encoders.bean(User.class);
			Dataset<User> userDF = userDs.as(userEncoder);
			userDF.foreach(new ForeachFunction<User>() {

				@Override
				public void call(User user) throws Exception {
					String query = String.format("INSERT INTO users(%s) VALUES(%s)", user.getColumns(), user.getArgs());
					PreparedStatement stm = connection.prepareStatement(query);
					int i = 0;
					for (Object object : user.getParameters()) {
						stm.setObject(++i, object);
					}
					stm.execute();
				}
			});
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void importSellingItems(SparkSession spark) {
		Dataset<Row> sellingItemDs = spark
				.sql("SELECT unix_timestamp(order_date) as orderDate, product_id as productId,"
						+ " SUM(quantity) AS quantity, SUM(revenue) AS revenue, Sum(revenue*(1-discount)) AS netRevenue "
						+ " FROM selling_order GROUP BY order_date, product_id ORDER BY orderDate");
		try {
			connection.createStatement();
			Encoder<SellingItem> categoryEncoder = Encoders.bean(SellingItem.class);
			Dataset<SellingItem> sellingItemDF = sellingItemDs.as(categoryEncoder);
			sellingItemDF.foreach(new ForeachFunction<SellingItem>() {

				@Override
				public void call(SellingItem sellingItem) throws Exception {
					String query = String.format("INSERT INTO selling_items(%s) VALUES(%s)", sellingItem.getColumns(),
							sellingItem.getArgs());
					PreparedStatement stm = connection.prepareStatement(query);
					int i = 0;
					for (Object object : sellingItem.getParameters()) {
						stm.setObject(++i, object);
					}
					stm.execute();
				}
			});
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void importProduct(SparkSession spark) {
		Dataset<Row> productDs = spark.sql(
				"SELECT product_id as id, name as productName , price, subcategory, category, store_id as storeId FROM product");
		try {
			connection.createStatement();
			Encoder<Product> categoryEncoder = Encoders.bean(Product.class);
			Dataset<Product> productDF = productDs.as(categoryEncoder);
			productDF.foreach(new ForeachFunction<Product>() {

				@Override
				public void call(Product product) throws Exception {
					String query = "INSERT INTO products(id, product_name, price, subcategory, category, store_id) VALUES(?,?,?,?,?,?)";
					PreparedStatement stm = connection.prepareStatement(query);
					int i = 0;
					for (Object object : product.getParameters()) {
						stm.setObject(++i, object);
					}
					stm.execute();
				}
			});
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void importCategory(SparkSession spark) {
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

		// Extract category
		// importCategory(spark);
		// importProduct(spark);
		// importSellingItems(spark);
//		importUser(spark);
//		 importProductAge(spark);
//		 importOrderStatistic(spark);
		 importUserStatistic(spark);
	}
}
