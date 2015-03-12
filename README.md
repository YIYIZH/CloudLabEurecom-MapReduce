# CloudLabEurecom
URL: https://github.com/michiard/CLOUDS-LAB

1. Exercise 1: WordCount
- Problem: Count the occurrences of each word in a text file
- Solution: 3 possible implementations
    + ** Basic ** - WordCount.java
    + ** In memory combiner ** - WordCountIMC.java
    + ** Combiner ** - WordCountCombiner.java
- Questions:
    + How does the number of reducers affect performance? How many reducers can be executed in parallel?
    Increasing #Reducers will:
        * increase framework overhead (not good) (eg: network overhead)
        * but increase the load balancing and lower the cost of failures (good)
    For general cases:
    #Reducers can be executed in parallel depends on #CPU, #Cores in cluster, and also depends on #unique_key to be delivered to reducers
        #Tasks run in parallel per CPU = #Cores * #HW-threads per core (eg HyperThreading)


    + Use the JobTracker web interface to examine Job counters: can you explain the differences among the three variants of this exercise? For example, look at the amount of bytes shuffled by Hadoop
    In memory combiner:
        * Reduces #map output records due to the in-memory combiner technique -> somehow reduces #shuffle bytes
        * Be careful with memory caching (manually), less overhead (materialization of output)
    Combiner:
        * combines map output records significantly => significantly reduces #shuffle bytes (reduce network traffic)

    + Zipf's law states that given some corpus of natural language utterances, the frequency of any word is inversely proportional to its rank in the frequency table. Thus the most frequent word will occur approximately twice as often as the second most frequent word, three times as often as the third most frequent word, etc. For example, in the *Brown Corpus of American English* text, the word "*the*" is the most frequently occurring word, and by itself accounts for nearly 7% of all word occurrences. The second-place word "*of*" accounts for slightly over 3.5% of words, followed by "*and*". Only 135 vocabulary items are needed to account for half the Brown Corpus. (wikipedia.org)
    Can you explain how does the distribution of words affect your Job?
    => input data is skewed
    => imbalance load between reducers (straggler problem):
        * high load at reducers processing most frequent words => slower, finish later
        * low load at reducers processing lowest frequent words => faster, finish earlier, have to wait
    => overall, the performance decreases
    => requires a better version of Partitioner

2. Exercise 2: Words co-occurrence
- Problem: build the term co-occurrence (n*n) matrix
- Solution:
    + ** Pairs **
    + ** Stripes **
- Question:
    ** Pairs **
    + How does the number of reducer influence the behavior of the Pairs approach?
    Because the pair approach will emit a very large amount of intermediate values. If #reducer is small, reducers will be suffered from work load
    The pair approach brings more parallelism, but also increases communication cost

    + Why does `TextPair` need to be Comparable?
    Because we have to group the intermediate values at reducer by key, which is a TextPair object

    + Can you use the implemented reducers as *Combiner*?
    Yes, because the operations sum are distributive

    ** Stripes **
    + Can you use the implemented reducers as *Combiner*?
    No, because the key input and key output of Reducer is different. The key output is word pair

    + Do you think Stripes could be used with the in-memory combiner pattern?
    Yes, we can

    + How does the number of reducer influence the behavior of the Stripes approach?
    Increasing #Reducers will:
        * increase framework overhead (not good) (eg: network overhead)
        * but increase the load balancing and lower the cost of failures (good)

    + Using the Jobtracker Web Interface, compare the shuffle phase of *Pair* and *Stripes* design patterns.
    Stripes has much less shuffle data than pair (more compact)

    + Why `StringToIntMapWritable` is not Comparable (differently from `TextPair`)?
    We used StringToIntMapWritable to emit value, not the key. We didn't make any comparison or sort on the value

3. Exercise 3: ** Order Inversion ** to solve Relative term co-occurrence
- Questions:
Answer the following questions. In answering the questions below, consider the role of the combiner.
+ Do you think the Order Inversion approach is 'faster' than a naive approach with multiple jobs? For example, consider implementing a compound job in which you compute the numerator and the denominator separately, and then perform the computation of the relative frequency
Yes, it's faster than chaining multiple jobs. It will cost a lot I/O disk to write intermediate output for the next jobs in the chain.

+ What is the impact of the use of a 'special' compound key on the amounts of shuffled bytes?
Even increases the shuffled bytes if using without combiner. If we use combiner, the impact is not noticeable

+ How does the default partitioner works with `TextPair`? Can you imagine a different implementation that does not change the Partitioner?
the default partitioner is HashPartitioner which hashes both 2 fields of the TextPair key.
If we don't want to change partitioner => use stripes method

+ For each key, the reducer receives its marginal before the co-occurence with the other words. Why?
Because of the sort phase: we sort on TextPair objects such that the objects with marginal have higher order than the ones without

4. Exercise 4: Joins
    + ** Distributed Cache Join **
    + ** Reduce-side Join **
