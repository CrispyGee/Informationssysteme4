package de.hfu;

import java.sql.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by IMTT on 15.11.2016.
 */
public class TableImpl {

    private Connection connection;
    private PreparedStatement documentPs;
    private PreparedStatement wordcountPs;
    private PreparedStatement wordPs;
    private PreparedStatement query1Ps;

    public TableImpl(Connection connection) throws SQLException {
        this.connection = connection;
        this.documentPs = connection.prepareStatement("insert into documents (type, url, domain, time, hash) values (?, ?, ?, ?, ?)");
        this.wordcountPs = connection.prepareStatement("insert into wordcounts (doc_id, word_id, count) " +
                "values((select id from documents where hash=?), (select id from words where word=?), ?)");
        this.wordPs = connection.prepareStatement("insert into words (word) values (?)");
        this.query1Ps = connection.prepareStatement("SELECT ds.time AS time, sum(wcs.count) as count " +
                "FROM wordcounts AS wcs " +
                "LEFT JOIN documents AS ds " +
                "    ON ds.id = wcs.doc_id " +
                "LEFT JOIN words AS ws " +
                "    ON ws.id = wcs.word_id " +
                "WHERE " +
                "    ws.word = ? " +
                "    AND " +
                "    ds.time >= ? " +
                "    AND " +
                "    ds.time <= ? " +
                "GROUP BY " +
                "    ds.time " +
                "; ");
    }

    public void insertDocument(String type, String url, String domain, Date time, String hash) throws SQLException {
        try {
            documentPs.setString(1, type);
            documentPs.setString(2, url);
            documentPs.setString(3, domain);
            documentPs.setDate(4, time);
            documentPs.setString(5, hash);
            documentPs.executeUpdate();
        } catch (SQLException e) {
            //ignore duplicate keys
            if (e.getErrorCode()!=-239){
                throw e;
            }
        }
    }

    public void insertWordcount(int count, String hash, String word) throws SQLException {
//        System.out.println("Insert count " + count + " for hash " + hash + " for word " + word);
        try {
            wordcountPs.setString(1, hash);
            wordcountPs.setString(2, word);
            wordcountPs.setInt(3, count);
            wordcountPs.executeUpdate();
        } catch (SQLException e) {
            //ignore duplicate keys
            if (e.getErrorCode()!=-239){
                throw e;
            }
        }
    }

    public void insertWord(String word) throws SQLException {
        try {
            wordPs.setString(1, word);
            wordPs.executeUpdate();
        } catch (SQLException e) {
            //ignore duplicate keys
            if (e.getErrorCode()!=-239){
                throw e;
            }
        }

    }

    public Map<Date, Integer> query1(String word, Date from, Date to) throws SQLException {
        query1Ps.setString(1, word);
        query1Ps.setDate(2, from);
        query1Ps.setDate(3, to);
        ResultSet rs = query1Ps.executeQuery();
        Map<Date, Integer> dateCountmap = new HashMap<Date, Integer>();
        while (rs.next()) {
            int count = rs.getInt("count");
            Date time = rs.getDate("time");
//            if (dateCountmap.containsKey(time)){
//                dateCountmap.put(time, dateCountmap.get(time)+count);
//            }
//            else{
            dateCountmap.put(time, count);
//            }
        }
        return dateCountmap;
    }

    public LinkedHashMap<String, Integer> query2(String word, Date from, Date
            to, int top) throws SQLException {
        String query2String = "SELECT ds.url AS url, wcs.count AS count " +
                "FROM wordcounts AS wcs " +
                "LEFT JOIN documents AS ds " +
                "   ON ds.id = wcs.doc_id " +
                "LEFT JOIN words AS ws " +
                "   ON ws.id = wcs.word_id " +
                "WHERE " +
                "   ws.word = ? " +
                "   AND " +
                "   ds.time >= ? " +
                "   AND " +
                "   ds.time <= ? " +
                "ORDER BY " +
                "   count DESC " +
                "LIMIT "+ top + " " +
                "; ";
        PreparedStatement query2Ps = connection.prepareStatement(query2String);
        query2Ps.setString(1, word);
        query2Ps.setDate(2, from);
        query2Ps.setDate(3, to);
        ResultSet rs = query2Ps.executeQuery();
        LinkedHashMap<String, Integer> wordCountOrdered = new LinkedHashMap<String, Integer>();
        while (rs.next()) {
            int count = rs.getInt("count");
            String url = rs.getString("url").trim();
            wordCountOrdered.put(url, count);
        }
        return wordCountOrdered;
    }

    public void close() {

    }
}
