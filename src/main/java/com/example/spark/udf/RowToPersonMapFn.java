package com.example.spark.udf;

import com.example.spark.SparkJavaExample2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class RowToPersonMapFn implements MapFunction<Row, SparkJavaExample2.Person> {
    @Override
    public SparkJavaExample2.Person call(Row row) throws Exception {
        SparkJavaExample2.Person person = new SparkJavaExample2.Person();
        person.setAge(row.getAs("age") != null ? Math.toIntExact(row.getAs("age")) : 0);
        person.setName(row.getAs("name"));
        return person;
    }
}
