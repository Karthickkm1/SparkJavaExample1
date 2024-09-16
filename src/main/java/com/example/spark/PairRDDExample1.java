package com.example.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;

import java.util.List;
import java.util.ListIterator;

public class PairRDDExample1 {

    List<Person> personList;
    Dataset<Person> personDs;

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("JoinExample1")
                .master("local[*]")
                .getOrCreate();

        PairRDDExample1 pairRDDExample1 = new PairRDDExample1();
        pairRDDExample1.prepareData(spark);
        pairRDDExample1.operations(spark);

    }

    void operations(SparkSession sparkSession) {
        personDs.show();

        JavaRDD<Person> javaRDD1 = personDs.toJavaRDD();
        for (Person next : javaRDD1.collect()) {
            System.out.println(next);
        }

    }


    void prepareData(SparkSession sparkSession) throws AnalysisException {
        personList = List.of(
                new Person(1, "John", "Maths", 70),
                new Person(2, "Mike","English", 60),
                new Person(3, "Jack","Maths", 90),
                new Person(4, "Adam", "English", 75),
                new Person(5, "Joe","Maths", 87),
                new Person(6, "Joel","Science", 90)
        );

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        personDs = sparkSession.createDataset(personList, personEncoder);
    }
}


