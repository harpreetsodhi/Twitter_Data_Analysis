import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

public class DistributedTransactions extends Thread{

    private Connection localConnection;
    private Connection remoteConnection;
    private Statement localStatement;
    private Statement remoteStatement;
    private static Hashtable<String, TableLock> hm;
    private static ArrayList<String> distributedQuerySequence;

    public static void distributedQuerySequence(){
        hm = new Hashtable<>();
        hm.put("olist_customers_dataset", new TableLock(null, null));
        hm.put("olist_geolocation_dataset", new TableLock(null, null));
        hm.put("olist_order_items_dataset", new TableLock(null, null));
        hm.put("olist_order_payments_dataset", new TableLock(null, null));
        hm.put("olist_order_reviews_dataset", new TableLock(null, null));
        hm.put("olist_orders_dataset", new TableLock(null, null));
        hm.put("olist_products_dataset", new TableLock(null, null));
        hm.put("olist_sellers_dataset", new TableLock(null, null));
        hm.put("product_category_name_translation", new TableLock(null, null));

        distributedQuerySequence = new ArrayList<>();
        distributedQuerySequence.add("START TRANSACTION");
        distributedQuerySequence.add("select * from olist_customers_dataset WHERE customer_zip_code_prefix=1151;");
        distributedQuerySequence.add("UPDATE olist_customers_dataset SET customer_city = 'Amritsar' WHERE customer_zip_code_prefix=1151;");
        distributedQuerySequence.add("UPDATE olist_geolocation_dataset SET geolocation_city = 'Amritsar' WHERE geolocation_zip_code_prefix=1151;");
        distributedQuerySequence.add("UPDATE olist_products_dataset SET product_photos_qty=5 WHERE product_id='003938452c98ff9ab28e9e7b4bfe97ab';");
        distributedQuerySequence.add("INSERT INTO product_category_name_translation VALUES('produits de beauté','cosmetics');");
        distributedQuerySequence.add("DELETE FROM olist_sellers_dataset where seller_id='024b564ae893ce8e9bfa02c10a401ece';");
        distributedQuerySequence.add("COMMIT");
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

    public void run(){
        try {
            localStatement.execute(distributedQuerySequence.get(0));
            remoteStatement.execute(distributedQuerySequence.get(0));

            checkReadLock("olist_customers_dataset");
            localStatement.execute(distributedQuerySequence.get(1));

            checkWriteLock("olist_customers_dataset");
            localStatement.execute(distributedQuerySequence.get(2));

            checkWriteLock("olist_geolocation_dataset");
            localStatement.execute(distributedQuerySequence.get(3));

            checkWriteLock("olist_products_dataset");
            remoteStatement.execute(distributedQuerySequence.get(4));

            checkWriteLock("product_category_name_translation");
            remoteStatement.execute(distributedQuerySequence.get(5));

            checkWriteLock("olist_sellers_dataset");
            remoteStatement.execute(distributedQuerySequence.get(6));

            localStatement.execute(distributedQuerySequence.get(7));
            remoteStatement.execute(distributedQuerySequence.get(7));

            releaseLocks();
            closeConnection();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    private void checkWriteLock(String tableName) {
        while (true) {
            if (hm.get(tableName).lock == null || hm.get(tableName).lock.isEmpty()) {
                hm.put(tableName, new TableLock("write", Long.toString(Thread.currentThread().getId())));
                break;
            } else if (hm.get(tableName).lock.equals("read") && hm.get(tableName).reference.equals(Long.toString(Thread.currentThread().getId()))) {
                hm.get(tableName).lock = "write";
                break;
            } else if (hm.get(tableName).lock.equals("write") && hm.get(tableName).reference.equals(Long.toString(Thread.currentThread().getId()))) {
                break;
            }
        }
    }

    private void checkReadLock(String tableName) {
            while (true) {
                if (hm.get(tableName).lock == null || hm.get(tableName).lock.isEmpty()) {
                    hm.put(tableName, new TableLock("read", Long.toString(Thread.currentThread().getId())));
                    break;
                } else if (hm.get(tableName).lock.equals("read") && hm.get(tableName).reference.equals(Long.toString(Thread.currentThread().getId()))) {
                    break;
                } else if (hm.get(tableName).lock.equals("read") && !hm.get(tableName).reference.equals(Long.toString(Thread.currentThread().getId()))) {
                    hm.get(tableName).reference = Long.toString(Thread.currentThread().getId());
                    break;
                } else if (hm.get(tableName).lock.equals("write") && hm.get(tableName).reference.equals(Long.toString(Thread.currentThread().getId()))) {
                    break;
                }
            }
        }

    private void releaseLocks(){
        for (String key: hm.keySet()){
            if (hm.get(key).reference !=null && hm.get(key).reference.equals(Long.toString(Thread.currentThread().getId()))) {
                hm.put(key, new TableLock("", ""));
            }
        }
    }

    public void closeConnection(){
        try{
            if (localConnection != null) localConnection.close();
            if (remoteConnection != null) remoteConnection.close();
            if (localStatement != null) localStatement.close();
            if (remoteStatement != null) remoteStatement.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
