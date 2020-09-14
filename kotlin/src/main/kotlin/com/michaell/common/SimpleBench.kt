fun simpleBench(warmupIterations: Int = 5, testIterations: Int = 100, block: () -> Unit): Double {
    repeat(warmupIterations) { block() }
    val testIterationResults = LongArray(testIterations)
    for (i in 0 until testIterations) {
        val s = System.currentTimeMillis()
        block()
        val e = System.currentTimeMillis()
        testIterationResults[i] = e - s
    }
    return testIterationResults.average()
}
