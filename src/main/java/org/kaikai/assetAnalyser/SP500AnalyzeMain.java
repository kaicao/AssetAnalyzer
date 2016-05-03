package org.kaikai.assetAnalyser;

import com.opencsv.CSVReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by kaicao on 03/05/16.
 */
public class SP500AnalyzeMain {

  public static void main(String[] args) throws Exception {
    SparkConf conf = new SparkConf()
        .setMaster("local") // runs Spark on one thread on the local machine, without connecting to a cluster
        .setAppName("AssetAnalyzer");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> sp500CSV = sc.textFile("src/main/resources/sp500_history_daily.csv");
    String headerLine = sp500CSV.first();
    JavaRDD<SP500Data> sp500Data = sp500CSV
        .filter(line -> !line.equals(headerLine))
        .map(line -> {
          try (CSVReader reader = new CSVReader(new StringReader(line))) {
            return new SP500Data(reader.readNext());
          }
        }).cache();
    JavaDoubleRDD openData = sp500Data.mapToDouble(SP500Data::getOpen);
    JavaDoubleRDD closeData = sp500Data.mapToDouble(SP500Data::getClose);
    JavaDoubleRDD volumeData = sp500Data.mapToDouble(SP500Data::getVolume);
    System.out.println("==========================");
    System.out.println("S&P 500 Open data:");
    printData(openData);
    System.out.println("==========================");
    System.out.println("S&P 500 Close data:");
    printData(closeData);
    System.out.println("==========================");
    System.out.println("S&P 500 Volumn data:");
    printData(volumeData);
    System.out.println("==========================");
    JavaDoubleRDD changeData = sp500Data.mapToDouble(data -> data.getClose() - data.getOpen());
    System.out.println("==========================");
    System.out.println("S&P 500 Daily change data:");
    printData(changeData);
    System.out.println("==========================");
  }

  public static class SP500Data implements Serializable {
    private static final long serialVersionUID = 8350697257905153890L;
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private final long date;
    private final double open;
    private final double high;
    private final double low;
    private final double close;
    private final long volume;
    private final double adjClose;

    public SP500Data(String[] data) {
      if (data.length != 7) {
        throw new IllegalArgumentException("Not valid data " + data);
      }
      try {
        this.date = DATE_FORMAT.parse(data[0]).getTime();
        this.open = Double.valueOf(data[1]);
        this.high = Double.valueOf(data[2]);
        this.low = Double.valueOf(data[3]);
        this.close = Double.valueOf(data[4]);
        this.volume = Long.valueOf(data[5]);
        this.adjClose = Double.valueOf(data[6]);
      } catch (ParseException e) {
        throw new IllegalArgumentException("Not valid date " + data[0]);
      }
    }

    public long getDate() {
      return date;
    }

    public double getOpen() {
      return open;
    }

    public double getHigh() {
      return high;
    }

    public double getLow() {
      return low;
    }

    public double getClose() {
      return close;
    }

    public long getVolume() {
      return volume;
    }

    public double getAdjClose() {
      return adjClose;
    }
  }

  private static void printData(JavaDoubleRDD data) {
    System.out.println("Mean " + data.mean());
    System.out.println("Count " + data.count());
    System.out.println("Variance " + data.variance());
    System.out.println("Standard deviation " + data.stdev());
    System.out.println("Sample Variance " + data.sampleVariance());
    System.out.println("Sample Std. " + data.sampleStdev());
  }
}
