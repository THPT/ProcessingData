import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class UserStatistic implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6180883973977567033L;
	int id;
	int userId;
	Date orderDate;
	double spend;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public Date getOrderDate() {
		return orderDate;
	}

	public void setOrderDate(Date orderDate) {
		this.orderDate = orderDate;
	}

	public double getSpend() {
		return spend;
	}

	public void setSpend(double spend) {
		this.spend = spend;
	}

	public String getArgs() {
		return "?,?,?,?";
	}

	public String getColumns() {
		return "id, user_id, order_date, spend";
	}

	public List<Object> getParameters() {
		return Arrays.asList(id, userId, orderDate, spend);
	}
}
