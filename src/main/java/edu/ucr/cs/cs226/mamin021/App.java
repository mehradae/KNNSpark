package edu.ucr.cs.cs226.mamin021;

/**
 * Hello world!
 *
 */
public class App
{
    public static void main(final String[] args ) throws Exception
    {

       //Checking input format

        if(args.length!=4){
            System.out.print((char)27 + "[31m" +"This is not a correct input format!\n"+(char)27 + "[32m"+
                    "Correct format : <"+(char)27 + "[1m" + "input path"+(char)27 +"[0m"+(char)27 + "[32m" + "> <"
                    + (char)27 +"[1m" + "Xpoint in query"+(char)27 + "[0m"+(char)27 + "[32m"+"> <"+
                    (char)27 + "[1m"+"Ypoint in query"+(char)27 + "[0m"+(char)27 + "[32m"+"> <"
                    +(char)27 + "[1m"+"K factor"+(char)27 + "[0m"+(char)27 + "[32m"+">"+(char)27 + "[0m");
            return;
        }


        //Spark RDD
       new SparkRDD(args);

        //SPARK SQL
       new SparkSQL(args);

    }



}
