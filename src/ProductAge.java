import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ProductAge implements Serializable {
	String productId;
	int age;

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

	public String getArgs() {
		return "?,?";
	}

	public String getColumns() {
		return "product_id, age";
	}

	public List<Object> getParameters() {
		Object[] list = new Object[]{productId, age};

		return Arrays.asList(list);
	}
}
