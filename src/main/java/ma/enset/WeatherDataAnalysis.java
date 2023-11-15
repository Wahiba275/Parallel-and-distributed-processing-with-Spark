package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WeatherDataAnalysis {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Weather Data Analysis").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String pathToFile = "/home/bouzyan/2020.csv";
        JavaRDD<String> data = sc.textFile(pathToFile);
        JavaRDD<String[]> filteredData = data
                .filter(line -> !line.startsWith("ID"))
                .map(line -> line.split(","))
                .filter(values -> values[2].equals("TMIN") || values[2].equals("TMAX"));
        // Calculate tminAvg
        JavaPairRDD<String, Tuple2<Integer, Integer>> tminTemps = filteredData
                .filter(values -> values[2].equals("TMIN"))
                .mapToPair(values -> new Tuple2<>(values[2], new Tuple2<>(Integer.parseInt(values[3]), 1)))
                .reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()));

        JavaRDD<Double> tminAvg = tminTemps
                .mapValues(sumCount -> (double)sumCount._1() / sumCount._2())
                .values();
        // Calculate tmaxAvg
        JavaPairRDD<String, Tuple2<Integer, Integer>> tmaxTemps = filteredData
                .filter(values -> values[2].equals("TMAX"))
                .mapToPair(values -> new Tuple2<>(values[2], new Tuple2<>(Integer.parseInt(values[3]), 1)))
                .reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()));

        JavaRDD<Double> tmaxAvg = tmaxTemps
                .mapValues(sumCount -> (double)sumCount._1() / sumCount._2())
                .values();
        double tminAverage = tminAvg.collect().stream().mapToDouble(x -> x).average().orElse(Double.NaN);
        double tmaxAverage = tmaxAvg.collect().stream().mapToDouble(x -> x).average().orElse(Double.NaN);

        System.out.println("Average minimum temperature: " + tminAverage);
        System.out.println("Average maximum temperature: " + tmaxAverage);

        JavaRDD<Integer> tmaxValues= filteredData.filter(values -> values[2].equals("TMAX")).map(values -> Integer.parseInt(values[3]));
        int highestTmax = tmaxValues.reduce(Math::max);
        JavaRDD<Integer> tminValues= filteredData.filter(values -> values[2].equals("TMIN")).map(values -> Integer.parseInt(values[3]));
        int lowestTmin = tminValues.reduce(Math::min);

        System.out.println("Highest Maximum Temperature (TMAX): " + highestTmax);
        System.out.println("Lowest Minimum Temperature (TMIN): " + lowestTmin);

        JavaPairRDD<String, Integer> maxTempsByStation = filteredData
                .filter(values -> values[2].equals("TMAX"))
                .mapToPair(values -> new Tuple2<>(values[0], Integer.parseInt(values[3])))
                .reduceByKey(Math::max);

        List<Tuple2<Integer, String>> topStations = maxTempsByStation
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .take(5);

        JavaPairRDD<String, Integer> minTempsByStation = filteredData
                .filter(values -> values[2].equals("TMIN"))
                .mapToPair(values -> new Tuple2<>(values[0], Integer.parseInt(values[3])))
                .reduceByKey(Math::min);

        List<Tuple2<Integer, String>> bottomStations = minTempsByStation
                .mapToPair(Tuple2::swap)
                .sortByKey(true)
                .take(5);
        topStations.forEach(station -> System.out.println("Hot Station: " + station._2() + " with max temp: " + station._1()));
        bottomStations.forEach(station -> System.out.println("Cold Station: " + station._2() + " with min temp: " + station._1()));




    }
}
