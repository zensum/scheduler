package scheduler

import franz.ProducerBuilder
import franz.WorkerBuilder
import java.nio.ByteBuffer
import java.util.PriorityQueue
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.atomic.AtomicBoolean
import se.zensum.idempotenceconnector.IdempotenceStore
import java.util.concurrent.CompletableFuture

// A message to publish
data class ToPublish(val topic: String,
                     val key: String?,
                     val data: ByteBuffer)

// A scheduled task
data class ScheduledTask(val id: Long,
                         val time: Long,
                         val data: ToPublish): Comparable<ScheduledTask> {
    override fun compareTo(other: ScheduledTask): Int = when {
        this.time > other.time -> 1
        this.time < other.time -> -1
        else -> 0
    }
}

fun readScheduledTask(x: ByteArray): ScheduledTask =
    ScheduledTask(1, 1, ToPublish("rhee", "normies", ByteBuffer.allocate(1)))

val idem = IdempotenceStore("scheduler")

fun epoch() = System.currentTimeMillis() / 1000

inline fun swallowInterrupted(fn: () -> Unit)  {
    try { fn() }
    catch (ex: InterruptedException) {}
}

// Optimistic retry, keeps track of a number of threads in a critical section.
// If tripped they are all interrupted and the task is retried.
class OptimisticRetry {
    private val notify = ConcurrentSkipListSet<Thread>()
    fun guarded(fn: () -> Unit) {
        val th = Thread.currentThread()
        try {
            // After we are added to notify we
            // may get an interrupted exception anytime.
            // Before we call fn(), Interrupts don't
            // warrant a retry as nothing has been done.
            swallowInterrupted {
                notify.add(th)
            }
            var complete = false
            while(!complete) {
                swallowInterrupted {
                    fn()
                    // Flag that we're are done and stop retrying
                    complete = true
                }
            }
        } finally {
            // under any other circumstances remove us from notify
            notify.remove(th)
        }
    }
    fun trip() {
        notify.forEach {
            it.interrupt()
        }
    }
}

// The queue task represents the priority heap used for storing tasks,
// it allows waiting until the top most item is ripe for removing
class Queue {
    private val closed = AtomicBoolean(false)
    private val tasks = PriorityQueue<ScheduledTask>()
    private val retry = OptimisticRetry()
    fun add(t: ScheduledTask) = tasks.add(t).also { retry.trip() }
    fun takeBelow(l: Long): ScheduledTask? = tasks.peek().let {
        if (it != null && it.time < l) tasks.poll() else null
    }
    fun waitAsRequired() {
        retry.guarded {
            val now = epoch()
            val sleepEnd = tasks.peek()?.time ?: Long.MAX_VALUE
            val sleepUntil = sleepEnd - now
            if (sleepUntil > 0) {
                Thread.sleep(sleepUntil * 1000)
            }
        }
    }
    fun closed() = closed.get()
}

fun queueWorker(q: Queue, onItem: (ScheduledTask) -> Unit) = Thread {
    while(!q.closed()) {
        val t = epoch()
        var item: ScheduledTask? = q.takeBelow(t)
        while (item != null) {
            onItem(item)
            item = q.takeBelow(t)
        }
        q.waitAsRequired()
    }
}.also { it.start() }

// Gets tasks for publishing, checks idempotence then publishes.
class Publisher {
    private val p = ProducerBuilder.ofByteArray.create()
    fun publish(t: ScheduledTask) {
        idem.containsAsync(t.id.toString())
            .thenCompose {
                if (!it.isFound()) {
                    val d = t.data
                    p.sendAsync(d.topic, d.key, d.data.array()).thenAccept {
                        // Worked!
                    }
                } else {
                    CompletableFuture<Any?>().also { it.complete(null) }.thenAccept {}
                }
            }
    }
}

fun runConsumer(q: Queue) {
    WorkerBuilder.ofByteArray.handlePiped {
        it
            .map { it.value() }
            .map { readScheduledTask(it) }
            .advanceIf { !idem.contains(it.id.toString()) } // Ugly longs
            .execute { q.add(it) }
            .success()
            .end()
    }.start()
}
fun main(args: Array<String>) {
    val p = Publisher()
    val q = Queue()
    queueWorker(q, p::publish)
    runConsumer(q)
}