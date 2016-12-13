import java.io.Serializable;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;

public class UserStatistic implements Serializable {
	private static final long serialVersionUID = 6180883973977567033L;
	long userId;
	long orderDate;
	double spend;

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public long getOrderDate() {
		return orderDate;
	}

	public void setOrderDate(long orderDate) {
		this.orderDate = orderDate;
	}

	public double getSpend() {
		return spend;
	}

	public void setSpend(double spend) {
		this.spend = spend;
	}

	public String getArgs() {
		return "?,?,?";
	}

	public String getColumns() {
		return "user_id, order_date, spend";
	}

	public List<Object> getParameters() {
		Object[] list = new Object[]{userId, new Date((long) orderDate * 1000), spend};

		return Arrays.asList(list);
	}
}
