package com.example.spark.udf;

import com.example.spark.SparkJavaExample2;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class AgeFilter implements FilterFunction<Row> {
    @Override
    public boolean call(Row row) throws Exception {
        return row.getAs("age") != null;
    }
}
