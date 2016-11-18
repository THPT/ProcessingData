import java.io.Serializable;

public class Category implements Serializable {
	private static final long serialVersionUID = 1L;
	String categoryName;

	public String getCategoryName() {
		return categoryName;
	}

	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}
}
