package com.michaell

import simpleBench
import java.io.File
import java.util.concurrent.*


const val POISON_PILL = "POISON_PILL"

class ThreadedWordCounter(private val countersNum: Int) : AutoCloseable {
    private val queue = LinkedBlockingQueue<String>()
    private var threadPool = Executors.newFixedThreadPool(countersNum)

    val totalCount = ConcurrentHashMap<String, Int>()

    init {
        repeat(countersNum) {
            threadPool.submit(WordCounter(queue, totalCount))
        }
    }

    fun consume(str: String) {
        queue.add(str)
    }

    override fun close() {
        repeat(countersNum) { queue.add(POISON_PILL) }
        threadPool.shutdown()
        while (!threadPool.isTerminated) {
        }
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val avg = simpleBench {
                val threadedWC = ThreadedWordCounter(12)
                File("data/data.csv").bufferedReader().lines().parallel().forEach {
                    threadedWC.consume(it)
                }
                threadedWC.close()
            }
            println("$avg/ms")
        }
    }

}

private class WordCounter(val queue: BlockingQueue<String>, val totalCount: ConcurrentHashMap<String, Int>) : Runnable {
    override fun run() {
        while (!Thread.currentThread().isInterrupted) {
            val str = queue.take()
            if (str == POISON_PILL) return

            str.toLowerCase()
                .split(',')
                .flatMap { it.split(' ') }
                .forEach {
                    val key = "${it}1"
                    if (totalCount.containsKey(key)) totalCount[key] = totalCount.getValue(key) + 1
                    else totalCount[key] = 1
                }

        }
    }
}

