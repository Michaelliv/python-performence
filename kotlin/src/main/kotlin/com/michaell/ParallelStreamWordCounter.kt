package com.michaell

import simpleBench
import java.io.File
import java.util.concurrent.ConcurrentHashMap

object ParallelStreamWordCounter {
    @JvmStatic
    fun main(args: Array<String>) {
        val avg = simpleBench {
            val counts = ConcurrentHashMap<String, Int>()
            File("../data.csv")
                .bufferedReader()
                .lines().parallel()
                .flatMap { it.toLowerCase().split(", ").stream() }
                .flatMap { it.split(' ').stream() }
                .forEach {
                    if (counts.containsKey(it)) counts[it] = counts.getValue(it) + 1
                    else counts[it] = 1
                }
        }
        println("$avg/ms")
    }
}