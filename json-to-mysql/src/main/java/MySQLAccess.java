import org.json.simple.*;

import java.io.PrintWriter;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class MySQLAccess {
    private Connection d_connect = null;
    private String d_user_name;
    private String d_password;
    private String d_server_name;
    private String d_db_name;

    public void close() throws Exception {
        System.out.println("Close database");
        try {
            if (d_connect != null) {
                d_connect.close();
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public MySQLAccess(String server, String db, String user, String password) {
        d_server_name = server;
        d_db_name = db;
        d_user_name = user;
        d_password = password;
    }

    private Connection getConnection() throws Exception {
        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.jdbc.Driver");
            String conn = "jdbc:mysql://" + d_server_name + "/" +
                    d_db_name+"?user="+d_user_name+"&password="+d_password;
            System.out.println("Connecting to database: " + conn);
            d_connect = DriverManager.getConnection(conn);
            System.out.println("Connected to database");
            return d_connect;
        } catch(Exception e) {
            throw e;
        }
    }

    public void addMasterData(String mysql_table_name, Map<String, String> mysql_field_array, JSONArray data) throws Exception {
        Connection connect = null;
        Statement product_info = null;
        StringBuffer sb = new StringBuffer();
        sb.append("insert into " + "`" + mysql_table_name + "` (");

        int n = mysql_field_array.size();
        // in order to match following values
        String[] keys = new String[n];
        //in order to user matching get method
        //String: 0 ; Number (int + double): 1; Date: 2;
        int[] types = new int[n];

        int k=0;
        for(Map.Entry<String, String> entry: mysql_field_array.entrySet()){
            String key = entry.getKey();
            String value = entry.getValue();
            keys[k] = key;
            types[k++] = detectType(value);
            if(k==n) sb.append("`" + key + "`)");
            else sb.append("`" + key + "`, ");
        }
        sb.append(" values\r\n");
        int i=0;
        for(Object o: data){

            JSONObject object = (JSONObject) o;
            sb.append("(");

            for(int j=0;j<n;j++){
                if(types[j]==2) {
                    String date = (String) object.get(keys[j]);
                    sb.append(formatDate(date));
                }
//                else if(types[j]==1){
//                    String num = (String) object.get(keys[j]);
//                    sb.append(numberProcess(num));
//                }
                else {
                    String curr = (String) object.get(keys[j]);
                    sb.append(stringProcess(curr));
                }
                if(j==n-1){
                    sb.delete(sb.length()-2, sb.length());
                    if(i!=data.size()-1) sb.append(")\r\n,");
                    else sb.append(");");
                }
            }
            System.out.println(sb.toString());
            i++;
        }
        try {
            connect = getConnection();
            connect.setAutoCommit(false);
            product_info = connect.createStatement();
            product_info.executeUpdate(sb.toString());
            PrintWriter writer = new PrintWriter("/Users/Zoe/Downloads/Utofun assignment/output.sql", "UTF8");
            writer.println(sb.toString());

//            //another method: batch process with prepareStatement
//            product_info = connect.prepareStatement(sql_string);
//
//            for(Master master: data){
//                product_info.setString(1, master.getDocument_id());
//                product_info.setString(2, master.getRecord_type());
//                product_info.setString(3, master.getCrfn());
//                product_info.setInt(4, master.getBorough());
//                product_info.setDate(5, new Date(master.getModified_date().getTime()));
//                product_info.setInt(6, master.getReel_year());
//                product_info.setInt(7, master.getReel_number());
//                product_info.setInt(8, master.getReel_page());
//                product_info.setDate(9, new Date(master.getGood_through_date().getTime()));
//                product_info.setDate(10, new Date(master.getDoc_date().getTime()));
//                product_info.setString(11, master.getDoc_type());
//                product_info.setDouble(12, master.getTransaction_amount());
//                product_info.setDate(13, new Date(master.getRecorded_date().getTime()));
//                product_info.setDouble(14, master.getPercent_transfer());
//                product_info.setString(15, master.getDescription());
//                product_info.addBatch();
//
//            }
//            int[] results = product_info.executeBatch(); // Execute every 1000 items.
//            System.out.println(product_info);
            
            connect.commit();
            writer.close();
        }
        catch(SQLException e )
        {
            System.out.println(e.getMessage());
            throw e;
        }
        finally
        {
            if (product_info != null) {
                product_info.close();
            }
            if (connect != null) {
                connect.close();
            }
        }
    }

    //date process to show yyyy-MM-dd HH:mm:ss format in query
    //sql date format is yyyy-MM-dd
    public String formatDate(String date) throws ParseException{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        Date parsedDate = sdf.parse(date);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String s = simpleDateFormat.format(parsedDate);
        return "'"+s+"', ";
    }

    //int or double process to extract null
//    public String numberProcess(String num){
//        double d = Double.valueOf(num);
//        if(d==0) return "null, ";
//        else return "'" + num + "', ";
//    }

    //string process to avoid '
    public String stringProcess(String s){
        if(s==null) return s+", ";
        s = s.replace("'", "\\'");
        return "'"+s+"', ";
    }

    //detect data type to String, int/double, or date
    public int detectType(String s){
        if(s.contains("varchar")) return 0;
        else if(s.contains("date")) return 2;
        else return 1;
    }
}
