import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ProcessingData {
	static Connection connection;

	private static Connection createConnection() throws Exception {
//		return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/grok?user=grok&password=grok&ssl=false");
		return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/grok?user=thpt&password=w5sU6BJK&ssl=false");
//		return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/grok?ssl=false");
	}

	public static void importStore(SparkSession spark) {
		Dataset<Row> storeDs = spark.sql("SELECT store_id as id, name, user_id as userId, location FROM selling_store");
		try {
			connection.createStatement();

			String tableName = "stores";
			String tempTableName = tableName + "_" + getTimeString();
			PreparedStatement stm = connection.prepareStatement("CREATE TABLE " + tempTableName
					+ "(" +
					"id serial PRIMARY KEY NOT NULL," +
					"name text," +
					"user_id int," +
					"location text" +
					")");
			stm.execute();

			Encoder<Store> storeEncoder = Encoders.bean(Store.class);
			Dataset<Store> storeDF = storeDs.as(storeEncoder);
			storeDF.foreach(new ForeachFunction<Store>() {
				@Override
				public void call(Store store) throws Exception {
					String query = String.format("INSERT INTO " + tempTableName + "(%s) VALUES(%s)", store.getColumns(), store.getArgs());
					PreparedStatement stm = connection.prepareStatement(query);
					int i = 0;
					for (Object object : store.getParameters()) {
						stm.setObject(++i, object);
					}
					stm.execute();
				}
			});

			swapTable(tableName, tempTableName);

			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void importUserStatistic(SparkSession spark) {
		Dataset<Row> orderStatisticDs = spark
				.sql("SELECT unix_timestamp(order_date) as orderDate, customer_id as userId, sum(revenue*(1-discount)) as spend "
						+ "FROM selling_order GROUP BY order_date, customer_id ORDER BY orderDate");
		try {
			connection.createStatement();

			String tableName = "user_statistics";
			String tempTableName = tableName + "_" + getTimeString();
			PreparedStatement stm = connection.prepareStatement("CREATE TABLE " + tempTableName
					+ "(" +
					"    id serial PRIMARY KEY NOT NULL," +
					"    user_id int," +
					"    order_date timestamp with time zone," +
					"    spend double precision" +
					")");
			stm.execute();

			Encoder<UserStatistic> userStatisticEncoder = Encoders.bean(UserStatistic.class);
			Dataset<UserStatistic> userStatisticDF = orderStatisticDs.as(userStatisticEncoder);
			userStatisticDF.foreach(new ForeachFunction<UserStatistic>() {

				@Override
				public void call(UserStatistic userStatistic) throws Exception {
					String query = String.format("INSERT INTO " + tempTableName + "(%s) VALUES(%s)",
							userStatistic.getColumns(), userStatistic.getArgs());
					PreparedStatement stm = connection.prepareStatement(query);
					int i = 0;
					for (Object object : userStatistic.getParameters()) {
						stm.setObject(++i, object);
					}
					stm.execute();
				}
			});

			swapTable(tableName, tempTableName);

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

			String tableName = "order_statistics";
			String tempTableName = tableName + "_" + getTimeString();
			PreparedStatement stm = connection.prepareStatement("CREATE TABLE " + tempTableName
					+ "(" +
					"    id serial PRIMARY KEY NOT NULL," +
					"    store_id int," +
					"    order_date timestamp with time zone," +
					"    revenue bigint," +
					"    total_order int" +
					")");
			stm.execute();

			Encoder<OrderStatistic> orderStatisticEncoder = Encoders.bean(OrderStatistic.class);
			Dataset<OrderStatistic> orderStatisticDF = orderStatisticDs.as(orderStatisticEncoder);
			orderStatisticDF.foreach(new ForeachFunction<OrderStatistic>() {

				@Override
				public void call(OrderStatistic user) throws Exception {
					String query = String.format("INSERT INTO " + tempTableName + "(%s) VALUES(%s)", user.getColumns(),
							user.getArgs());
					PreparedStatement stm = connection.prepareStatement(query);
					int i = 0;
					for (Object object : user.getParameters()) {
						stm.setObject(++i, object);
					}
					stm.execute();
				}
			});

			swapTable(tableName, tempTableName);

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

			String tableName = "product_ages";
			String tempTableName = tableName + "_" + getTimeString();
			PreparedStatement stm = connection.prepareStatement("CREATE TABLE " + tempTableName
					+ "(" +
					"    id serial PRIMARY KEY NOT NULL," +
					"    product_id text," +
					"    age int" +
					")");
			stm.execute();

			Encoder<ProductAge> productAgeEncoder = Encoders.bean(ProductAge.class);
			Dataset<ProductAge> productAgeDF = userDs.as(productAgeEncoder);
			productAgeDF.foreach(new ForeachFunction<ProductAge>() {

				@Override
				public void call(ProductAge productAge) throws Exception {
					String query = String.format("INSERT INTO " + tempTableName + "(%s) VALUES(%s)", productAge.getColumns(),
							productAge.getArgs());
					PreparedStatement stm = connection.prepareStatement(query);
					int i = 0;
					for (Object object : productAge.getParameters()) {
						stm.setObject(++i, object);
					}
					stm.execute();
				}
			});

			swapTable(tableName, tempTableName);

			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void importUser(SparkSession spark) {
		Dataset<Row> userDs = spark.sql(
				"SELECT user_id as id, age, gender, unix_timestamp(creation_date) as createdAt, location FROM user");
		try {
			connection.createStatement();

			String tableName = "users";
			String tempTableName = tableName + "_" + getTimeString();
			PreparedStatement stm = connection.prepareStatement("CREATE TABLE " + tempTableName
					+ "(" +
					"    id serial PRIMARY KEY NOT NULL," +
					"    age int," +
					"    gender int, " +
					"    location text," +
					"    created_at timestamp WITH time ZONE" +
					")");
			stm.execute();

			Encoder<User> userEncoder = Encoders.bean(User.class);
			Dataset<User> userDF = userDs.as(userEncoder);
			userDF.foreach(new ForeachFunction<User>() {

				@Override
				public void call(User user) throws Exception {
					String query = String.format("INSERT INTO " + tempTableName + "(%s) VALUES(%s)", user.getColumns(), user.getArgs());
					PreparedStatement stm = connection.prepareStatement(query);
					int i = 0;
					for (Object object : user.getParameters()) {
						stm.setObject(++i, object);
					}
					stm.execute();
				}
			});

			swapTable(tableName, tempTableName);

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

			String tableName = "selling_items";
			String tempTableName = tableName + "_" + getTimeString();
			PreparedStatement stm = connection.prepareStatement("CREATE TABLE " + tempTableName
					+ "(" +
					"    id serial PRIMARY KEY NOT NULL," +
					"    product_id text," +
					"    quantity int, " +
					"    revenue bigint, " +
					"    net_revenue double precision," +
					"    order_date timestamp with time zone" +
					")");
			stm.execute();

			Encoder<SellingItem> categoryEncoder = Encoders.bean(SellingItem.class);
			Dataset<SellingItem> sellingItemDF = sellingItemDs.as(categoryEncoder);
			sellingItemDF.foreach(new ForeachFunction<SellingItem>() {

				@Override
				public void call(SellingItem sellingItem) throws Exception {
					String query = String.format("INSERT INTO " + tempTableName + "(%s) VALUES(%s)", sellingItem.getColumns(),
							sellingItem.getArgs());
					PreparedStatement stm = connection.prepareStatement(query);
					int i = 0;
					for (Object object : sellingItem.getParameters()) {
						stm.setObject(++i, object);
					}
					stm.execute();
				}
			});

			swapTable(tableName, tempTableName);

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

			String tableName = "products";
			String tempTableName = tableName + "_" + getTimeString();
			PreparedStatement stm = connection.prepareStatement("CREATE TABLE " + tempTableName
					+ "(" +
					"    id text PRIMARY KEY NOT NULL," +
					"    product_name text," +
					"    price       bigint," +
					"    subcategory text," +
					"    category    text," +
					"    store_id     int" +
					")");
			stm.execute();

			Encoder<Product> categoryEncoder = Encoders.bean(Product.class);
			Dataset<Product> productDF = productDs.as(categoryEncoder);
			productDF.foreach(new ForeachFunction<Product>() {

				@Override
				public void call(Product product) throws Exception {
					String query = "INSERT INTO " + tempTableName + "(id, product_name, price, subcategory, category, store_id) VALUES(?,?,?,?,?,?)";
					PreparedStatement stm = connection.prepareStatement(query);
					int i = 0;
					for (Object object : product.getParameters()) {
						stm.setObject(++i, object);
					}
					stm.execute();
				}
			});

			swapTable(tableName, tempTableName);

			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void importCategory(SparkSession spark) {
		Dataset<Row> categories = spark.sql("SELECT DISTINCT category as categoryName FROM product");
		try {
			connection.createStatement();

			String tableName = "categories";
			String tempTableName = tableName + "_" + getTimeString();
			PreparedStatement stm = connection.prepareStatement("CREATE TABLE " + tempTableName
					+ "(id serial PRIMARY KEY NOT NULL, category_name text)");
			stm.execute();

			Encoder<Category> categoryEncoder = Encoders.bean(Category.class);
			Dataset<Category> categoryDF = categories.as(categoryEncoder);
			categoryDF.foreach(new ForeachFunction<Category>() {
				@Override
				public void call(Category t) throws Exception {
                    // TODO create temp table
					String query = "INSERT INTO " + tempTableName + "(category_name) VALUES(?)";
					PreparedStatement stm = connection.prepareStatement(query);
					stm.setString(1, t.categoryName);
					stm.execute();
				}
			});

            swapTable(tableName, tempTableName);

			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static String getTimeString() {
		return new SimpleDateFormat("ddMMyyHHmmss").format(new Date());
	}

	private static void swapTable(String tableName, String tempTableName) throws SQLException {
        PreparedStatement stm;
		stm = connection.prepareStatement("ALTER TABLE " + tableName + " RENAME TO " + tableName + "_bak_" + getTimeString());
		stm.execute();
		stm = connection.prepareStatement("ALTER TABLE " + tempTableName + " RENAME TO " + tableName);
		stm.execute();
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

		importCategory(spark);
		importProduct(spark);
		importSellingItems(spark);
		importUser(spark);
		importProductAge(spark);
		importOrderStatistic(spark);
		importUserStatistic(spark);
		importStore(spark);
	}
}
