package de.hfu;

import com.csvreader.CsvWriter;
import com.informix.jdbc.IfxConnection;
import com.informix.jdbc.IfxDriver;
import hfu.generator.Meta;
import hfu.generator.WordInfo;
import hfu.generator.WordInfos;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;

/**
 * Created by IMTT on 15.11.2016.
 */
public class Main {


    private static HashMap<String, String> hashUrls;

    public static void main(String args[]) throws Exception {
        String path = "D:\\Informationssysteme\\samples";
        parseData(path);
        parseSecondary(false,  "words", "urls");
        parseSecondary(true,  "wordsHashes", "urls");
    }

    public static void parseSecondary(boolean withHash, String file1, String file2) throws Exception{
        hashUrls = new HashMap<String, String>();
        FileInputStream fis = new FileInputStream("data");
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
        FileWriter allDataFile = new FileWriter(file1, true);
        FileWriter hashUrlFile = new FileWriter(file2, true);
        int i = 0;
        while (true) {
            String line = reader.readLine();
            if (line != null && !line.isEmpty()) {
                String[] split = line.split("\\|");
                String word = split[0];
                String time = split[1];
                String url = split[2];
                String count = split[3];
                if (withHash) {
                    String hash = convertToHash(url);
                    if (!hashUrls.containsKey(hash)) {
                        hashUrls.put(hash, url);
                        hashUrlFile.append(hash + "|" + url + "|\n");
                    }
                    String append = word + "|" + time + "|" + hash + "|" + count + "|\n";
                    allDataFile.append(append);
                }
                else{
                    String append = word + "|" + time + "|" + count + "|\n";
                    allDataFile.append(append);
                }
//                i++;
//                if (i % 10000 == 0) {
//                    System.out.println("at " + i);
//                    System.out.println(append);
//                }
            } else {
                break;
            }
        }
        reader.close();
        fis.close();
        allDataFile.close();
        hashUrlFile.close();
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

    public static void parseData(String path) throws Exception {
        String outputFile = "data";
        FileWriter fw = new FileWriter(outputFile, true);
        for (WordInfo info : new WordInfos(path)) {
            Meta meta = info.getMeta();
            String word = info.getWord();
            Date time = new Date(meta.getTime());
            String url = meta.getUri().trim();
            int count = info.getFrequency();
            TimeZone tz = TimeZone.getTimeZone("UTC");
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd"); // Quoted "Z" to indicate UTC, no timezone offset
            df.setTimeZone(tz);
            String isoDateString = df.format(time);
            fw.append(word + "|" + isoDateString + "|" + url + "|" + count + "|\n");
//            csvWriter.write(word);
//            csvWriter.write(isoDateString);
//            csvWriter.write(url);
//            csvWriter.write(Integer.toString(count));
//            csvWriter.endRecord();
        }
        fw.close();
    }

}
