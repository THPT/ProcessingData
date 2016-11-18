import java.io.Serializable;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;

public class SellingItem implements Serializable {
	String productId;
	long quantity;
	long revenue;
	double netRevenue;
	long orderDate;

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public long getQuantity() {
		return quantity;
	}

	public void setQuantity(long quantity) {
		this.quantity = quantity;
	}

	public long getRevenue() {
		return revenue;
	}

	public void setRevenue(long revenue) {
		this.revenue = revenue;
	}

	public double getNetRevenue() {
		return netRevenue;
	}

	public void setNetRevenue(double netRevenue) {
		this.netRevenue = netRevenue;
	}

	public long getOrderDate() {
		return orderDate;
	}

	public void setOrderDate(long orderDate) {
		this.orderDate = orderDate;
	}

	public String getArgs() {
		return "?,?,?,?,?";
	}

	public String getColumns() {
		return "product_id, quantity, revenue, net_revenue, order_date";
	}

	public List<Object> getParameters() {
		return Arrays.asList(productId, quantity, revenue, netRevenue, new Date((long) orderDate * 1000));
	}
}
