import java.io.Serializable;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;

public class User implements Serializable {
	long id;
	int age;
	int gender;
	String location;
	long createdAt;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public int getGender() {
		return gender;
	}

	public void setGender(int gender) {
		this.gender = gender;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public long getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(long createdAt) {
		this.createdAt = createdAt;
	}
	
	public String getArgs(){
		return "?,?,?,?,?";
	}
	
	public String getColumns(){
		return "id, age, gender, location, created_at";
	}

	public List<Object> getParameters() {
	    Object[] list = new Object[]{id, age, gender, location, new Date((long)createdAt*1000)};

	    return Arrays.asList(list);
	}
}
