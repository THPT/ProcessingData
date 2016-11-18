import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class Product implements Serializable {
	String id;
	String productName;
	long price;
	String subcategory;
	String category;
	long storeId;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
		this.productName = productName;
	}

	public long getPrice() {
		return price;
	}

	public void setPrice(long price) {
		this.price = price;
	}

	public String getSubcategory() {
		return subcategory;
	}

	public void setSubcategory(String subcategory) {
		this.subcategory = subcategory;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public long getstoreId() {
		return storeId;
	}

	public void setstoreId(long storeId) {
		this.storeId = storeId;
	}

	public List<Object> getParameters() {
		return Arrays.asList(id, productName, price, subcategory, category, storeId);
	}

}
