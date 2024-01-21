package dijkstra

import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.locks.reentrantLock
import java.util.*
import java.util.concurrent.Phaser
import kotlin.concurrent.thread

/**
 * @author Belousov Timofey
 */

private val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 -> Integer.compare(o1!!.distance, o2!!.distance) }

// Returns `Integer.MAX_VALUE` if a path has not been found.
fun shortestPathParallel(start: Node) {
    val workers = Runtime.getRuntime().availableProcessors()
    // The distance to the start node is `0`
    start.distance = 0
    // Create a priority (by distance) queue and add the start node into it
    val q = SuperPriorityQueue(workers, NODE_DISTANCE_COMPARATOR)
    q.add(start)
    q.activeNodes.incrementAndGet()
    // Run worker threads and wait until the total work is done
    val onFinish = Phaser(workers + 1) // `arrive()` should be invoked at the end by each worker
    repeat(workers) {
        thread {
            while (true) {
                val cur: Node? = q.poll()
                if (cur == null) {
                    if (q.activeNodes.value == 0) break else continue
                }
                for (e in cur.outgoingEdges) {
                    while (true) {
                        val curDistance = cur.distance
                        val curEDistance = e.to.distance
                        if (curEDistance > curDistance + e.weight) {
                            if (!e.to.casDistance(curEDistance, curDistance + e.weight)) continue
                            q.activeNodes.incrementAndGet()
                            q.add(e.to)
                        }
                        break
                    }
                }
                q.activeNodes.decrementAndGet()
                if (q.activeNodes.value < 0) throw IllegalStateException("WTF?")
            }
            onFinish.arrive()
        }
    }
    onFinish.arriveAndAwaitAdvance()
}


class SuperPriorityQueue<E>(workers: Int, private val comparator: Comparator<in E>) {
    companion object {
        private const val C = 3
    }

    val queues = Array(workers * C) { (PriorityQueue(1, comparator) to reentrantLock()) }
    val activeNodes = atomic(0)

    fun add(element: E) {
        while (true) {
            val (q, lock) = queues[queues.indices.random()]
            if (!lock.tryLock()) continue
            q.add(element)
            lock.unlock()
            return
        }
    }

    fun poll(): E? {
        while (true) {
            val i1 = queues.indices.random()
            var i2: Int
            do {
                i2 = queues.indices.random()
            } while (i2 == i1)

            val (q1, lock1) = queues[i1]
            val (q2, lock2) = queues[i2]

            val top1 = q1.peek()
            val top2 = q2.peek()

            val (targetQueue, targetLock) = if (top1 == null) {
                q2 to lock2
            } else if (top2 == null) {
                q1 to lock1
            } else if (comparator.compare(top1, top2) < 0) {
                q1 to lock1
            } else {
                q2 to lock2
            }

            if (!targetLock.tryLock()) continue

            val result = targetQueue.poll()

            targetLock.unlock()
            return result
        }
    }
}
