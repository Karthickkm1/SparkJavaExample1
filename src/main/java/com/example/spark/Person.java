package com.example.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Person implements Serializable {
    public int id;
    public String name;
    public String subject;
    public int marks;
}
