package ma.enset;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class AnalyseVentes {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Ventes Par Ville").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> ventes = sc.textFile("ventes.txt");
        JavaRDD<Tuple2<String, Double>> ventesParVille = ventes.map(s -> {
            String[] parts = s.split("\\s+");
            if (parts.length == 4) {
                return new Tuple2<>(parts[1], Double.parseDouble(parts[3]));
            } else {
                System.err.println("Ligne invalide: " + s);
                return new Tuple2<>("Invalid", 0.0);
            }
        });
        JavaRDD<Tuple2<String, Double>> filteredVentesParVille = ventesParVille.filter(tuple -> !tuple._1().equals("Invalid"));
        JavaPairRDD<String, Double> totalVentesParVille = filteredVentesParVille.mapToPair(
                tuple -> tuple
        ).reduceByKey((a, b) -> a + b);
        //totalVentesParVille.saveAsTextFile("output");
        totalVentesParVille.foreach(e-> System.out.println(e._1+" "+e._2));
        sc.close();

        //*********************** Part 2 ******************************
        int year = 2023;
        JavaRDD<String> ventesPourAnnee = ventes.filter(
                s -> s.startsWith(String.valueOf(year))
        );
        JavaPairRDD<String, Double> ventesParVille1 = ventesPourAnnee.mapToPair(
                s -> {
                    String[] parts = s.split("\\s+");
                    return new Tuple2<>(parts[1], Double.parseDouble(parts[3]));
                }
        );
        JavaPairRDD<String, Double> totalVentesParProduitEtVille = ventesParVille1.mapToPair(
                (PairFunction<Tuple2<String, Double>, String, Double>) t -> new Tuple2<>(t._1(), t._2())
        ).reduceByKey((a, b) -> a + b);
        //totalVentesParVille.saveAsTextFile("output");
        totalVentesParProduitEtVille.saveAsTextFile("output-" + year);
        totalVentesParProduitEtVille.foreach(e-> System.out.println(e._1+" "+e._2));
        sc.close();

    }
}
