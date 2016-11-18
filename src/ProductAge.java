import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ProductAge implements Serializable{
	int id;
	String productId;
	int age;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}
	
	public String getArgs(){
		return "?,?,?,?,?";
	}
	
	public String getColumns(){
		return "id, product_id, age";
	}

	public List<Object> getParameters() {
		return Arrays.asList(id, productId, age);
	}
}
