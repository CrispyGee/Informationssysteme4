package de.hfu;

import com.informix.jdbc.IfxConnection;
import com.informix.jdbc.IfxDriver;
import hfu.generator.Meta;
import hfu.generator.WordInfo;
import hfu.generator.WordInfos;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Map;

/**
 * Created by IMTT on 15.11.2016.
 */
public class Main {

    private static String ip;
    private static String port;
    private static String user;
    private static String password;

    private static Connection connectIfx() {
        try {
            Class.forName("com.informix.jdbc.IfxDriver");
            DriverManager.registerDriver((com.informix.jdbc.IfxDriver) Class.forName("com.informix.jdbc.IfxDriver").newInstance());
        } catch (Exception e) {
            System.out.println("ERROR: failed to load Informix JDBC driver.");
            e.printStackTrace();
            return null;
        }
        String database = "assignment04";
        String informixserver = "ol_informix1210";
        String url = "jdbc:informix-sqli://" + ip + ":" + port + "/" + database + ":INFORMIXSERVER="
                + informixserver
                + ";user=" + user + ";password=" + password;
        System.out.println(url);
        try {
            Connection ifxConnection = DriverManager.getConnection(url, user, password);
            return ifxConnection;
        } catch (SQLException e) {
            System.out.println("ERROR: failed to connect!");
            System.out.println("ERROR: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }


    public static void main(String args[]) throws Exception {
        String path = "D:\\Informationssysteme\\samples";
        if (args.length >= 3) {
            path = args[0];
            ip = args[1];
            port = args[2];
            user = args[3];
            password = args[4];
        }
        Connection ifxConn = connectIfx();
        TableImpl ifxTable = new TableImpl(ifxConn);
//        parseData(ifxTable, path);
        long t0 = System.currentTimeMillis();
        Map<Date, Integer> q1Res = ifxTable.query1("reddit", new Date(1353067200), new Date(System.currentTimeMillis()));
        long t1 = System.currentTimeMillis();
        System.out.println("Query 1: " + (t1 - t0) + " ms");
        System.out.println("Showing 5 sample results...");
        int i = 5;
        for (Map.Entry<Date, Integer> entry :
                q1Res.entrySet()) {
            if (i > 0) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
                i--;
            } else {
                break;
            }
        }
        long t2 = System.currentTimeMillis();
        Map<String, Integer> q2Res = ifxTable.query2("reddit", new Date(1353067200), new Date(System.currentTimeMillis()), 10);
        long t3 = System.currentTimeMillis();
        System.out.println("Query 2: " + (t3 - t2) + " ms");
        System.out.println("Showing 5 sample results...");
        i = 5;
        for (Map.Entry<String, Integer> entry :
                q2Res.entrySet()) {
            if (i > 0) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
                i--;
            } else {
                break;
            }
        }
        ifxConn.close();
    }

    public static void parseData(TableImpl ifxTable, String path) throws Exception {
        for (WordInfo info : new WordInfos(path)) {
            Meta meta = info.getMeta();
            String url = meta.getUri();
            String word = info.getWord();
            int frequency = info.getFrequency();
            String hash = convertToHash(url);
            Date time = new Date(meta.getTime());
            System.out.println("url: " + url + " hash: " + hash);
            ifxTable.insertDocument(meta.getType(), url, new String(), time, hash);
            ifxTable.insertWord(word);
            ifxTable.insertWordcount(frequency, hash, word);
        }
    }

    public static String convertToHash(String url) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(url.getBytes());
        byte byteData[] = md.digest();
        //convert the byte to hex format method 1
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < byteData.length; i++) {
            sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 32).substring(1));
        }
        return sb.toString();
    }

}
