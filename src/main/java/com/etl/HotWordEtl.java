package com.etl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class HotWordEtl {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");
        SparkConf conf = new SparkConf().setAppName("hot word").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> linesRdd =  jsc.textFile("hdfs://namenode:8020/student/student.txt");

        System.out.println(linesRdd.collect());


//        //map
//        JavaPairRDD<String,Integer> pairRdd = linesRdd.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                String word = s.split("\t")[2];
//                return new Tuple2<>(word,1);
//            }
//        });
//
//        //reduce
//        JavaPairRDD<String,Integer> result = pairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer + integer2;
//            }
//        });

//        List<Tuple2<String,Integer>> take = result.take(10);
//        for (Tuple2<String,Integer> tuple2 : take) {
//            System.out.println(tuple2._1 + "===" + tuple2._2);
//        }

//        JavaPairRDD<Integer,String> swapRdd = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
//            @Override
//            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                return stringIntegerTuple2.swap();
//            }
//        });
//
//        JavaPairRDD<Integer,String> sorted = swapRdd.sortByKey(false);


    }


}
