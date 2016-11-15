package de.hfu;

import com.informix.jdbc.IfxConnection;
import com.informix.jdbc.IfxDriver;
import hfu.generator.WordInfo;
import hfu.generator.WordInfos;
import java.sql.*;

/**
 * Created by IMTT on 15.11.2016.
 */
public class Main {

    private static String ip;
    private static String port;
    private static String user;
    private static String password;

    private static Connection connectIfx(){
        try
        {
            Class.forName("com.informix.jdbc.IfxDriver");
            DriverManager.registerDriver((com.informix.jdbc.IfxDriver)Class.forName("com.informix.jdbc.IfxDriver").newInstance());
        }
        catch (Exception e)
        {
            System.out.println("ERROR: failed to load Informix JDBC driver.");
            e.printStackTrace();
            return null;
        }
        //port 27017
        String database = "assignment04";
        String informixserver = "ol_informix1210";
        String url = "jdbc:informix-sqli://" + ip +":" +port +"/"+database + ":INFORMIXSERVER="
                + informixserver
        +";user=" +user +";password=" + password;
        System.out.println(url);
//        System.out.println("User: " + user);
//        System.out.println("Password: " + password);
        try
        {
            Connection ifxConnection= DriverManager.getConnection(url, user, password);
            return ifxConnection;
        }
        catch (SQLException e)
        {
            System.out.println("ERROR: failed to connect!");
            System.out.println("ERROR: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }


    public static void main(String args[]) throws SQLException {
        String path = "C:\\Users\\IMTT\\Downloads\\samples";
        if (args.length>=3){
            path = args[0];
            ip = args[1];
            port = args[2];
            user = args[3];
            password = args[4];
        }
        Connection ifxConn = connectIfx();
        TableImpl ifxTable = new TableImpl(ifxConn);
        for(WordInfo info : new WordInfos(path)) {
            Date time = new Date(info.getMeta().getTime());
            ifxTable.insertDocument(info.getMeta().getType(), info.getMeta().getUri(), new String(), time);
            ifxTable.insertWord(info.getWord());
            ifxTable.insertWordcount(info.getFrequency(), info.getMeta().getUri(), info.getWord());
        }
        ifxConn.close();
    }
}
