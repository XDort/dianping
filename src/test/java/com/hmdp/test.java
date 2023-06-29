package com.hmdp;

import org.springframework.boot.SpringBootVersion;
import org.springframework.core.SpringVersion;

public class test {
    public static void main(String[] args) {
        System.out.println(SpringVersion.getVersion());
        System.out.println(SpringBootVersion.getVersion());
    }
}
