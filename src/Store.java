import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class Store implements Serializable {
	long id;
	String name;
	long userId;
	String location;

	public long getId() { return id; }

	public void setId(long id) { this.id = id; }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getUserId() { return userId; }

	public void setUserId(long userId) { this.userId = userId; }

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getArgs() { return "?,?,?,?"; }

	public String getColumns() { return "id, name, user_id, location"; }

	public List<Object> getParameters() { return Arrays.asList(id, name, userId, location); }
}
