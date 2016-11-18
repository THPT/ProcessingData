import java.io.Serializable;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;

public class OrderStatistic implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5581773326771321911L;
	int id;
	int storeId;
	Date orderDate;
	long revenue;
	int totalOrder;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getStoreId() {
		return storeId;
	}

	public void setStoreId(int storeId) {
		this.storeId = storeId;
	}

	public Date getOrderDate() {
		return orderDate;
	}

	public void setOrderDate(Date orderDate) {
		this.orderDate = orderDate;
	}

	public long getRevenue() {
		return revenue;
	}

	public void setRevenue(long revenue) {
		this.revenue = revenue;
	}

	public int getTotalOrder() {
		return totalOrder;
	}

	public void setTotalOrder(int totalOrder) {
		this.totalOrder = totalOrder;
	}
	public String getArgs(){
		return "?,?,?,?,?";
	}
	
	public String getColumns(){
		return "id, store_id, order_date, revenue, total_order";
	}

	public List<Object> getParameters() {
		return Arrays.asList(id, storeId, orderDate, revenue, totalOrder);
	}
}
