public class DistributedTransactionMain {
    public static void main(String[] args) {
        DistributedTransactions.distributedQuerySequence();
        DistributedTransactions transaction1 = new DistributedTransactions();
        DistributedTransactions transaction2 = new DistributedTransactions();

        transaction1.connect();
        transaction2.connect();

        transaction1.start();
        transaction2.start();
    }
}
