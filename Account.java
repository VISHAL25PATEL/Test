class Account {
    private int accountNo;
    private double balance;

    // Constructor
    public Account(int accountNo, double balance) {
        this.accountNo = accountNo;
        this.balance = balance;
    }

    void deposit(double amount) throws InvalidAmountException {
        if (amount < 100) {
            throw new InvalidAmountException("Deposit amount must be at least 100");
        }
        balance += amount;
        System.out.println("Deposited: " + amount);
    }

    void withdraw(double amount) throws InvalidAmountException {
        if (amount < 100) {
            throw new InvalidAmountException("Withdraw amount must be at least 100");
        } else if (amount > balance) {
            System.out.println("Insufficient balance!");
        } else {
            balance -= amount;
            System.out.println("Withdrawn: " + amount);
        }
    }

    @Override
    public String toString() {
        return "Account No: " + accountNo + ", Balance: " + balance;
    }
}





// User-defined checked exception
class InvalidAmountException extends Exception {
    public InvalidAmountException(String message) {
        super(message);
    }
}









public class TestAccount {
    public static void main(String[] args) {
        Account acc = new Account(101, 1000);

        try {
            acc.deposit(50);   // This will throw exception
        } catch (InvalidAmountException e) {
            System.out.println(e.getMessage());
        }

        try {
            acc.withdraw(500); // Valid withdraw
        } catch (InvalidAmountException e) {
            System.out.println(e.getMessage());
        }

        System.out.println(acc);
    }
}
