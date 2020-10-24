import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public class EmbeddedTransactions {

    private Connection localConnection;
    private Connection remoteConnection;
    private Statement localStatement;
    private Statement remoteStatement;
    private ResultSet localResultSet;
    private ResultSet remoteResultSet;
    private static ArrayList<String> al;

    public static void sequence(){
        al = new ArrayList<>();
        al.add("START TRANSACTION");
        al.add("select * from olist_customers_dataset where customer_zip_code_prefix=01151;");
        al.add("UPDATE olist_customers_dataset SET customer_city = 'T1 city' WHERE customer_zip_code_prefix=01151;");
        al.add("UPDATE olist_customers_dataset SET customer_city = 'T2 city' WHERE customer_zip_code_prefix=01151;");
        al.add("COMMIT");
    }

    public void connect() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            localConnection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/customer_schema", "root", "password");
            remoteConnection = DriverManager.getConnection("jdbc:mysql://34.121.145.147/order_schema", "root", "B00833691");
            localConnection.setAutoCommit(false);
            remoteConnection.setAutoCommit(false);
            localStatement = localConnection.createStatement();
            remoteStatement = remoteConnection.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void performQuery(int sequence){
        try {
            localStatement.execute(al.get(sequence));
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public void closeConnection(){
        try{
            if (localConnection != null) localConnection.close();
            if (remoteConnection != null) remoteConnection.close();
            if (localStatement != null) localStatement.close();
            if (remoteStatement != null) remoteStatement.close();
            if (localResultSet != null) localResultSet.close();
            if (remoteResultSet != null) remoteResultSet.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
