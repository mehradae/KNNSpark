package edu.ucr.cs.cs226.mamin021;



import org.apache.spark.sql.*;


import java.time.LocalDate;
import java.time.LocalTime;


public class SparkSQL {

    private static final String IDcolumn ="_c0";
    private static final String Xcolumn ="_c1";
    private static final String Ycolumn ="_c2";

    public SparkSQL(String[] args) throws Exception{

        SparkSession session = SparkSession.builder().appName("KNNSpark")
                //.master("spark://mehrad-VirtualBox:7077")
                .master("local")
                .getOrCreate();
       session.sparkContext().setLogLevel("ERROR");

        Dataset<Row> dataset = session.read().option("delimiter",",").option("header", "false")
                .csv(args[0]);
        LocalTime lt = LocalTime.now();
        LocalDate ld = LocalDate.now();
        System.out.println("\n Spark SQL query results :");

                dataset.select(dataset.col(Xcolumn),dataset.col(Ycolumn),dataset.col(IDcolumn),
                functions.sqrt(functions.pow(dataset.col(Xcolumn).minus(args[1]),2).
                        plus(functions.pow(dataset.col(Ycolumn).minus(args[2]),2))).cast("String")
                        .alias("Distance")).orderBy("Distance").limit(Integer.parseInt(args[3]))
                        .rdd().saveAsTextFile("SparkSQL_result_"+ld+"_"+lt.getHour()+"-"+
                            lt.getMinute()+"-"+lt.getSecond());



    }

}
