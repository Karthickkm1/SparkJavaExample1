package com.example.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.List;

public class JoinExample1 {

    static List<Person> personList;
    static List<Team> teamList;
    static Dataset<Person> personDs;
    static Dataset<Team> teamDs;

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("JoinExample1")
                .master("local[*]")
                .getOrCreate();

        prepareData(spark);

        personDs.show();
        teamDs.show();

        var joinCondition = personDs.col("teamId") == teamDs.col("id");

        /* Join (inner, by default) */
        System.out.println("/* Join */");
        personDs.join(teamDs, personDs.col("teamId").equalTo(teamDs.col("id"))).show();
        teamDs.join(personDs, teamDs.col("id").equalTo(personDs.col("teamId"))).show();

        /* LeftJoin */
        System.out.println("/* LeftJoin */");
        personDs.join(teamDs, personDs.col("teamId").equalTo(teamDs.col("id")), "left_outer").show();
        teamDs.join(personDs, teamDs.col("id").equalTo(personDs.col("teamId")), "left_outer").show();

        /* OuterJoin */
        System.out.println("/* OuterJoin */");
        personDs.join(teamDs, personDs.col("teamId").equalTo(teamDs.col("id")), "outer").show();
        teamDs.join(personDs, teamDs.col("id").equalTo(personDs.col("teamId")), "outer").show();

        /* CrossJoin */
        System.out.println("/* CrossJoin */");
        personDs.crossJoin(teamDs).show(30);
        teamDs.crossJoin(personDs).show(30);

    }

    private static void prepareData(SparkSession sparkSession) throws AnalysisException {
        personList = List.of(
                new Person(1, "John", 1),
                new Person(2, "Mike", 2),
                new Person(3, "Jack", 3),
                new Person(4, "Adam", 1),
                new Person(5, "Joe", 2),
                new Person(6, "Joel", 6)
        );
        teamList = List.of(
                new Team(1, "Cricket"),
                new Team(2, "Football"),
                new Team(3, "Tennis"),
                new Team(4, "Basketball")
        );

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Encoder<Team> teamEncoder = Encoders.bean(Team.class);

        personDs = sparkSession.createDataset(personList, personEncoder);
        teamDs = sparkSession.createDataset(teamList, teamEncoder);

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Person implements Serializable {
        public int id;
        public String name;
        public int teamId;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Team implements Serializable {
        public int id;
        public String tName;
    }


}
