package de.hfu;

import com.csvreader.CsvWriter;
import com.informix.jdbc.IfxConnection;
import com.informix.jdbc.IfxDriver;
import hfu.generator.Meta;
import hfu.generator.WordInfo;
import hfu.generator.WordInfos;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by IMTT on 15.11.2016.
 */
public class Main {


    public static void main(String args[]) throws Exception {
        String path = "D:\\Informationssysteme\\samples";
        parseData(path);
    }

    public static void parseData(String path) throws Exception {
        String outputFile = "data.csv";
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outputFile, true), ',');
        for (WordInfo info : new WordInfos(path)) {
            Meta meta = info.getMeta();
            String word = info.getWord();
            Date time = new Date(meta.getTime());
            String url = meta.getUri();
            int count = info.getFrequency();
            TimeZone tz = TimeZone.getTimeZone("UTC");
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
            df.setTimeZone(tz);
            String isoDateString = df.format(time);
            csvWriter.write(word);
            csvWriter.write(isoDateString);
            csvWriter.write(url);
            csvWriter.write(Integer.toString(count));
            csvWriter.endRecord();
        }
        csvWriter.close();
    }

}
