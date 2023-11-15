package ma.enset;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RDDLineage {
    public static void main(String[] args) {

        List<String> studentNames = Arrays.asList(
                "Ahmed", "Ali", "Amina", "Farah", "Hassan",
                "Layla", "Mohamed", "Nour", "Omar", "Salma",
                "Youssef", "Zahra", "Karim", "Imane", "Hakim",
                "Samira", "Tariq", "Yasmin", "Amir", "Hana",
                "Malik", "Nadia", "Sami", "Rania", "Ibrahim",
                "Mona", "Adel", "Sana","Ali", "Faisal", "Lina",
                "Mustafa", "Aisha", "Khaled", "Dalia", "Wahiba","Rashid",
                "Leila", "Anwar", "Fatima", "Jamal", "Sara",
                "Ismail", "Hiba", "Mahmoud", "Noura", "Waleed",
                "Maha", "Abbas", "Lamia", "Yasin", "Samia",
                "Fadi", "Rima", "Harun", "Najwa", "Ziad",
                "Bilal", "Amira", "Said", "Ines", "Tarek","Wahiba");

        SparkConf conf = new SparkConf().setAppName("RDDLineage").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.parallelize(studentNames);
        JavaRDD<String> rdd2 = rdd1.flatMap(name -> Arrays.asList(name.split("\\s+")).iterator());
        List<String> results = rdd2.collect();
        System.out.println(results);
        JavaRDD<String> rdd3 = rdd2.filter(name -> name.startsWith("A"));
        JavaRDD<String> rdd4= rdd2.filter(name -> name.length()<5);
        JavaRDD<String> rdd5 = rdd2.filter(name -> name.endsWith("a"));
        JavaRDD<String> rdd6 = rdd3.union(rdd4);
        JavaPairRDD<String, Integer> rdd71 = rdd5.map(name -> name.toUpperCase()).mapToPair(name -> new Tuple2<>(name, 1));
        JavaPairRDD<String, Integer> rdd81 = rdd6.map(name -> name.toLowerCase()).mapToPair(name -> new Tuple2<>(name, 1));
        JavaPairRDD<String, Integer> rdd8 = rdd81.reduceByKey((a, b) -> a + b);
        JavaPairRDD<String, Integer> rdd7 = rdd71.reduceByKey((a, b) -> a + b);
        JavaRDD<String> rdd9 = rdd8.keys().union(rdd7.keys());
        JavaRDD<String> rdd10 = rdd9.sortBy(key -> key, false, 1);
        List<String> sortedResults = rdd10.collect();
        System.out.println(sortedResults);
        sc.close();








    }

}
