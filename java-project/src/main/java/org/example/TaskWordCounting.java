package org.example;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class TaskWordCounting {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskWordCounting.class);

    static class WordMapper implements FlatMapFunction<String, String>
    {
        @Override
        public Iterator<String> call(String s) {
            if (s == null) return Collections.emptyIterator();
            // Replace any non-alphanumeric character with a space, lowercase, then split on whitespace
            String cleaned = s.toLowerCase().replaceAll("[^a-z0-9]+", " ").trim();
            if (cleaned.isEmpty()) return Collections.emptyIterator();
            String[] tokens = cleaned.split("\\s+");
            List<String> words = new ArrayList<>();
            for (String t : tokens) {
                if (!t.isEmpty()) {
                    words.add(t);
                }
            }
            return words.iterator();
        }

    }

    public static void run(boolean local) throws IOException {

        long start = System.currentTimeMillis();

        LOGGER.info("Starting the task");

        SparkConf sparkConf = null;

        String datasetFileName = "dataset-wordcount.txt";
        String datasetFilePath ="../datasets/" + datasetFileName;
        String applicationName = "WordCount";
        String hdfsDatasetPath = "hdfs://namenode:9000/datasets/";
        String sparkMaster = "spark://spark-master:7077";

        Date t0 = new Date(); //Mark the start timestamp

        if(local){
            sparkConf = new SparkConf().setAppName(applicationName).setMaster("local[8]").set("spark.executor.instances", "1").set("spark.executor.instances", "10") .set("spark.executor.memory", "4g");
        }else {
            datasetFilePath = hdfsDatasetPath + datasetFileName;
            sparkConf = new SparkConf().setAppName(applicationName).setMaster(sparkMaster);
        }

        JavaSparkContext sparkContext =  new JavaSparkContext(sparkConf);
        //sparkContext.setLogLevel("WARN");
        LOGGER.info("Loading text file");
        JavaRDD<String> textFile = sparkContext.textFile(datasetFilePath, 3);

        //=================================== Your code now =========================================

        //Step-A: using the available textFile, create a flat map of words by calling the WordMapper.
        LOGGER.info("Flat mapping to create word list");
        JavaRDD<String> words = textFile.flatMap(new WordMapper());

        //-------------------------------------------------------------------------------------------
        Date t1 = new Date();
        LOGGER.info("Word list created in {}ms", t1.getTime()-t0.getTime());


        //Step B: Now invoke a mapping function that will create key value-pair for each word in the list
        LOGGER.info("Mapping function");
        JavaPairRDD<String, Integer> pairs = words.mapToPair(w -> new Tuple2<>(w, 1));

        //-------------------------------------------------------------------------------------------
        Date t2 = new Date();
        LOGGER.info("Mapping invoked in {}ms", t2.getTime()-t1.getTime());

        //Step C: Invoke a Reduce function that will sum up the values (against each key)
        LOGGER.info("Reducing function");
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(Integer::sum);

        //-------------------------------------------------------------------------------------------
        Date t3 = new Date();
        LOGGER.info("Reduce invoked in {}ms", t3.getTime()-t2.getTime());

        //Step D: Finally, output the counts for each word
        LOGGER.info("Collecting to driver");
        List<Tuple2<String, Integer>> output = counts.collect();
        LOGGER.info("Unique words: {}", output.size());
        for (int i = 0; i < Math.min(20, output.size()); i++) {
            Tuple2<String, Integer> t = output.get(i);
            LOGGER.info("word='{}' count={}", t._1(), t._2());

        }

        //-------------------------------------------------------------------------------------------
        Date t4 = new Date();
        LOGGER.info("Application completed in {}ms", t4.getTime()-t0.getTime());

        //If you want you can save the counts to a hdfs file
        if(!local) {
            //counts.repartition(1).saveAsTextFile("hdfs://namenode:9000/output/counts.txt");
        }

        long end = System.currentTimeMillis();
        System.out.println("Total execution time: " + (end - start) + " ms");
        sparkContext.stop();
        sparkContext.close();
    }
}
