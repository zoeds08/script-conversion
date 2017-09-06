

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class ConvertionApplication {

    private static ObjectMapper mapper = new ObjectMapper();

    static final String d_user_name = "root";
    static final String d_password = "19930823";
    static final String d_server_name = "127.0.0.1:3306";
    static final String d_db_name = "Test";
    static final MySQLAccess mySQLAccess = new MySQLAccess(d_server_name,d_db_name,d_user_name,d_password);

    public static void main(String[] args){
        String mysql_table_name = "master";
//        String[] mysql_field_array = {"document_id", "record_type", "crfn", "borough", "modified_date", "reel_year", "reel_number", "reel_page", "good_through_date", "doc_date", "doc_type", "transaction_amount", "recorded_date", "percent_transfer", "description"};
        Map<String, String> map = initiateMap();
        String json_items = "/Users/Zoe/Downloads/Utofun assignment/sample_input.json";
        convert(mysql_table_name,map,json_items);
    }

    public static void convert(String mysql_table_name, Map<String, String> mysql_field_array, String json_items){

        JSONParser parser = new JSONParser();
        try {
            JSONArray data = (JSONArray) parser.parse(new FileReader(json_items));
            mySQLAccess.addMasterData(mysql_table_name, mysql_field_array,data);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
//        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
//        mapper.setDateFormat(dateFormat);
//        try {
//            //1. read json from file
//            InputStream is = new FileInputStream(new File(json_items));
//            //2. convert json to java object array
//            Master[] data = mapper.readValue(is, Master[].class);
//            mySQLAccess.addMasterData(mysql_table_name, mysql_field_array,data);
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (JsonParseException e) {
//            e.printStackTrace();
//        } catch (JsonMappingException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    public static Map<String, String> initiateMap(){
        Map<String, String> map = new HashMap<String, String>();//key: field; value: type
        map.put("borough", "int(11)");
        map.put("crfn", "varchar(255)");
        map.put("transaction_amount", "double");
        map.put("doc_date", "date");
        map.put("doc_type", "varchar(255)");
        map.put("document_id", "varchar(255)");
        map.put("good_through_date", "date");
        map.put("modified_date", "date");
        map.put("percent_transfer", "double");
        map.put("record_type", "varchar(255)");
        map.put("recorded_date", "date");
        map.put("reel_number", "int(11)");
        map.put("reel_page", "int(11)");
        map.put("reel_year", "int(11)");
        map.put("description", "varchar(255)");
        return map;
    }

}
