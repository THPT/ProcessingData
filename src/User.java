import java.io.Serializable;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;

public class User implements Serializable {
	int id;
	int age;
	int gender;
	String location;
	Date createdAt;

	public int getId() {
		return id;
	}

	public void setId(int id) {
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

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}
	
	public String getArgs(){
		return "?,?,?,?,?";
	}
	
	public String getColumns(){
		return "id, age, gender, location, created_at";
	}

	public List<Object> getParameters() {
		return Arrays.asList(id, age, gender, location, createdAt);
	}
}
