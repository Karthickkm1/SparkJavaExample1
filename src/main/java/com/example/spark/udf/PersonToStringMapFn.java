package com.example.spark.udf;

import com.example.spark.SparkJavaExample2;
import org.apache.spark.api.java.function.MapFunction;

public class PersonToStringMapFn implements MapFunction<SparkJavaExample2.Person, String> {
    @Override
    public String call(SparkJavaExample2.Person person) throws Exception {
        return String.format("%S : %d", person.name, person.age+1);
    }
}
