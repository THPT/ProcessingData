import java.io.Serializable;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;

public class OrderStatistic implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5581773326771321911L;
	long storeId;
	long orderDate;
	long revenue;
	long totalOrder;

	public long getStoreId() {
		return storeId;
	}

	public void setStoreId(long storeId) {
		this.storeId = storeId;
	}

	public long getOrderDate() {
		return orderDate;
	}

	public void setOrderDate(long orderDate) {
		this.orderDate = orderDate;
	}

	public long getRevenue() {
		return revenue;
	}

	public void setRevenue(long revenue) {
		this.revenue = revenue;
	}

	public long getTotalOrder() {
		return totalOrder;
	}

	public void setTotalOrder(long totalOrder) {
		this.totalOrder = totalOrder;
	}

	public String getArgs() {
		return "?,?,?,?";
	}

	public String getColumns() {
		return "store_id, order_date, revenue, total_order";
	}

	public List<Object> getParameters() {
		return Arrays.asList(storeId, new Date((long) orderDate * 1000), revenue, totalOrder);
	}
}
