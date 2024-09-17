package com.example.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.List;

import static org.apache.spark.sql.functions.*;

public class PairRDDExample1 {

    List<Person> personList;
    Dataset<Person> personDs;

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JoinExample1")
                .master("local[*]")
                .getOrCreate();

        PairRDDExample1 pairRDDExample1 = new PairRDDExample1();
        pairRDDExample1.prepareData(spark);
        pairRDDExample1.operations();

    }

    void operations() {
        personDs.show();

        JavaRDD<Person> personRDD = personDs.toJavaRDD();

        // Aggregation
        JavaPairRDD<String, Integer> marksPairRDD = personRDD.mapToPair(person -> {
            String name = person.getName();
            int marks = person.getMarks();
            return new Tuple2<>(name, marks);
        });

        System.out.println("Marks pair RDD");
        marksPairRDD.collect().forEach(System.out::println);

        System.out.println("Reduce by key");
        marksPairRDD.reduceByKey(Integer::sum).collect().forEach(System.out::println);

        System.out.println("Group by key");
        JavaPairRDD<String, Iterable<Integer>> marksPairRDD1 = marksPairRDD.groupByKey();
        marksPairRDD1.collect().forEach(rdd -> {
            System.out.println("Name : "+rdd._1);
            System.out.println("Marks : ");
            rdd._2.forEach(System.out::println);
        });

        System.out.println("SQL - Group by name");
        Dataset<Row> sum = personDs.groupBy(col("id"), col("name"))
                .sum("marks")
                .withColumnRenamed("sum(marks)", "Total Marks")
                .withColumnRenamed("id", "pid")
                .sort(col("id").asc(), col("Total Marks").desc());
        sum.show();

        System.out.println("SQL - Sort");
        personDs.sort(col("name"), col("marks").desc()).show();

        System.out.println("SQL - Aggregation");
        personDs.groupBy(col("name")).agg(avg("marks"), max("marks")).show();

    }

    void prepareData(SparkSession sparkSession) {
        personList = List.of(
                new Person(1, "John", "Maths", 70),
                new Person(2, "Mike","English", 60),
                new Person(3, "Jack","Maths", 90),
                new Person(4, "Adam", "English", 75),
                new Person(5, "Joe","Maths", 87),
                new Person(6, "Joel","Science", 90),
                new Person(1, "John", "English", 60),
                new Person(2, "Mike","Maths", 70)
        );

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        personDs = sparkSession.createDataset(personList, personEncoder);
    }
}


