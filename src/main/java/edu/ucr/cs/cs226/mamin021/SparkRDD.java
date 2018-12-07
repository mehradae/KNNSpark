package edu.ucr.cs.cs226.mamin021;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.time.LocalDate;
import java.time.LocalTime;

public class SparkRDD {
    public SparkRDD(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("SparkKnn")
                .setMaster("local");
        JavaSparkContext JSC = new JavaSparkContext(conf);
        JavaRDD<String> RDD = JSC.textFile(args[0]);
        JavaPairRDD<Double, String> pairs = RDD.mapToPair((PairFunction<String, Double, String>) s -> {
            String[] points = s.split(",");
            double Xpoint = Double.parseDouble(args[1]);
            double Ypoint = Double.parseDouble(args[2]);

            Double distance = Math.sqrt((Math.pow(Double.parseDouble(points[1]) - Xpoint, 2)
                    + Math.pow(Double.parseDouble(points[2]) - Ypoint, 2)));
            return new Tuple2<>(distance, s);
        });
        int k = Integer.parseInt(args[3]);
        JavaPairRDD<Double, String> list = pairs.sortByKey(true);


        System.out.println((char) 27 + "[1m" + (char) 27 + "[32m" + "Spark RDD results :" + (char) 27 + "[0m");
        LocalTime lt = LocalTime.now();
        LocalDate ld = LocalDate.now();
        FileWriter fw = new FileWriter("KNN_results_" + ld + "_" + lt.getHour() + "-" +
                lt.getMinute() + "-" + lt.getSecond() + ".txt");
        PrintWriter pw = new PrintWriter(fw);
        list.take(k).forEach(data -> {
            try {
                System.out.println(data._1 + "\t" + data._2);
                pw.println(data._1 + "\t" + data._2);
            } catch (Exception ex) {
                System.out.print("An error Occured writting the file:\n" + ex);
            }
        });
        pw.close();
    }
}
