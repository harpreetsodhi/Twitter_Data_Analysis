public class EmbeddedTransactionMain {
    public static void main(String[] args) {
        EmbeddedTransactions.embedQuerySequence();
        EmbeddedTransactions transaction1 = new EmbeddedTransactions();
        EmbeddedTransactions transaction2 = new EmbeddedTransactions();

        transaction1.connect();
        transaction2.connect();

        transaction1.performQuery(0);
        transaction2.performQuery(0);

        transaction1.performQuery(1);
        transaction2.performQuery(1);

        transaction1.performQuery(2);
        transaction2.performQuery(3);

        transaction2.performQuery(4);
        transaction1.performQuery(4);

        transaction1.closeConnection();
        transaction2.closeConnection();
    }
}
