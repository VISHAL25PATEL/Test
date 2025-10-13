class Customer {
    private int customerId;
    private String name;
    private String email;
    private String mobile;

    // Constructor
    public Customer(int customerId, String name, String email, String mobile) {
        this.customerId = customerId;
        this.name = name;
        this.email = email;
        this.mobile = mobile;
    }

    void print() {
        System.out.println("Customer ID: " + customerId);
        System.out.println("Name: " + name);
        System.out.println("Email: " + email);
        System.out.println("Mobile: " + mobile);
    }
}









class NRICustomer extends Customer {
    private String residingCountry;

    // Constructor
    public NRICustomer(int customerId, String name, String email, String mobile, String residingCountry) {
        super(customerId, name, email, mobile); // Call parent constructor
        this.residingCountry = residingCountry;
    }

    // Override print method
    @Override
    void print() {
        super.print(); // Print customer details
        System.out.println("Residing Country: " + residingCountry);
    }
}





public class TestCustomer {
    public static void main(String[] args) {
        NRICustomer n1 = new NRICustomer(101, "Vishal Patel", "vishal@gmail.com", "9876543210", "USA");
        n1.print();
    }
}
