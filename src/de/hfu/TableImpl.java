package de.hfu;

import com.ziclix.python.sql.connect.Connect;

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
    private PreparedStatement query2Ps;

    public TableImpl(Connection connection) throws SQLException {
        this.connection=connection;
        this.documentPs = connection.prepareStatement("insert into documents (type, url, domain, time) values (?, ?, ?, ?)");
        this.wordcountPs = connection.prepareStatement("insert into wordcounts (doc_id, word_id, count) select documents.id, words.id, ? from" +
                "documents, words where documents.url=? and words.word=?");
        this.wordPs = connection.prepareStatement("insert or ignore into words (word) values (?)");
        this.query1Ps = connection.prepareStatement("");
        this.query2Ps = connection.prepareStatement("");
    }

    public void insertDocument(String type, String url, String domain, Date time) throws SQLException {
        documentPs.setString(1, type);
        documentPs.setString(2, url);
        documentPs.setString(3, domain);
        documentPs.setDate(4, time);
        documentPs.executeUpdate();
    }

    public void insertWordcount(int count, String url, String word) throws SQLException {
        wordcountPs.setInt(1, count);
        wordcountPs.setString(2, url);
        wordcountPs.setString(3, word);
        wordcountPs.executeUpdate();
    }

    public void insertWord(String word) throws SQLException {
        wordPs.setString(1, word);
        wordPs.executeUpdate();
    }

    public Map<Date,Integer> query1(String word, String from, String to) throws SQLException {
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

    public LinkedHashMap<String,Integer> query2(String word, String from, String
            to, int top) throws SQLException {
        ResultSet rs = query2Ps.executeQuery();
        LinkedHashMap<String, Integer> wordCountOrdered = new LinkedHashMap<String, Integer>();
        while (rs.next()) {
            int count = rs.getInt("count");
            String url = rs.getString("url");
            wordCountOrdered.put(url, count);
        }
        return wordCountOrdered;
    }

    public void close() {

    }
}
