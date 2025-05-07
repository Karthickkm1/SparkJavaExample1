package com.example.spark;

import com.example.spark.udf.AgeFilter;
import com.example.spark.udf.PersonToStringMapFn;
import com.example.spark.udf.RowToPersonMapFn;
import lombok.Data;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;

import static org.apache.spark.sql.functions.col;

public class SparkJavaExample2 {

    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkJavaExample2")
                .master("local[*]")
                .getOrCreate();

        mapFn(spark);
    }

    private static void mapFn(SparkSession sparkSession) throws AnalysisException {

        Dataset<Row> inputDS = sparkSession.read().json("src/main/resources/people.json");
        inputDS.show();

        inputDS.printSchema();

        inputDS.select(col("age")).show();

        inputDS.filter(col("age")
                .gt(20)
                .and(col("name").equalTo("Andy")))
                .show();

        inputDS.filter((FilterFunction<Row>) row -> row.getAs("age") == null).show();

        System.out.println("UDF implementation.. ");

        inputDS.filter(new AgeFilter())
                .show();

        Dataset<String> mapStrDs = inputDS.map((MapFunction<Row, String>) row -> row.getString(1), Encoders.STRING());

        Encoder<Person> personEncoder = Encoders.bean(Person.class);

        System.out.println("UDF - RowToPersonMapFn ");
        inputDS.map(new RowToPersonMapFn(), personEncoder)
                .show();

        Dataset<Person> personDS = inputDS.map(new RowToPersonMapFn(), personEncoder);

        System.out.println("UDF - RowToPersonMapFn - Filter - Map");
        inputDS.filter(new AgeFilter())
                .map(new RowToPersonMapFn(), personEncoder)
                .show();

        System.out.println("UDF - PersonToStringMapFn - Map");
        personDS.map(new PersonToStringMapFn(), Encoders.STRING())
                .show();

        System.out.println("UDF - PersonToStringMapFn - Map1");
        personDS.filter((FilterFunction<Person>) Person::filterAge)
                .map((MapFunction<Person, String>) Person::increaseAge, Encoders.STRING())
                .show();

    }

    @Data
    public static class Person implements Serializable {
        public int age;
        public String name;

        public static String increaseAge(Person person) {
            return String.format("%S : %d", person.name, person.age + 1);
        }

        public static boolean filterAge(Person person) {
            return person.age != 0;
        }
    }
}
