package scheduler

import franz.ProducerBuilder
import franz.WorkerBuilder
import kotlinx.coroutines.experimental.runBlocking
import java.nio.ByteBuffer
import se.zensum.idempotenceconnector.IdempotenceStore
import java.util.Random
import java.util.concurrent.DelayQueue
import java.util.concurrent.Delayed
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

// A message to publish
data class ToPublish(val topic: String,
                     val key: String?,
                     val data: ByteBuffer)

// A scheduled task
data class ScheduledTask(val id: Long,
                         private val time: Long,
                         val data: ToPublish): Delayed {
    override fun compareTo(other: Delayed?): Int =
        getDelay(TimeUnit.SECONDS)
            .compareTo(other!!.getDelay(TimeUnit.SECONDS))
    override fun getDelay(unit: TimeUnit?): Long =
        unit!!.convert(fromNow(time), TimeUnit.SECONDS)

}

fun readScheduledTask(x: ByteArray): ScheduledTask =
    ScheduledTask(1, 1, ToPublish("rhee", "normies", ByteBuffer.allocate(1)))

val idem = IdempotenceStore("scheduler")

fun epoch() = System.currentTimeMillis() / 1000
fun fromNow(time: Long) =  time - epoch()

// The queue task represents the priority heap used for storing tasks,
// it allows waiting until the top most item is ripe for removing
class Queue {
    private val tasks = DelayQueue<ScheduledTask>()
    private val closed = AtomicBoolean(false)
    fun add(t: ScheduledTask) = !closed() && tasks.add(t)
    fun take() = tasks.take()
    fun close() = closed.lazySet(true)
    fun closed() = closed.get()
}

fun queueWorker(q: Queue, onItem: suspend (ScheduledTask) -> Unit) = Thread { runBlocking {
    while(!q.closed()) {
        val p = q.take()


        onItem(p)
    }
}}.also { it.start() }

private val producer = ProducerBuilder.ofByteArray.create()
fun publish(t: ScheduledTask) {
    val found = idem.contains(t.id.toString())
    if (found) {
        return
    }
    val d = t.data
    val f = producer.sendAsync(d.topic, d.key, d.data.array())
    f.thenAccept {
        idem.put(t.id.toString())
    }
}

fun runConsumer(q: Queue) =
    WorkerBuilder.ofByteArray.handlePiped {
        it
            .map { it.value() }
            .map { readScheduledTask(it) }
            .advanceIf { !idem.contains(it.id.toString()) } // Ugly longs
            .execute { q.add(it) }
            .success()
            .end()
    }.start()

val rnd = Random()
val tp = ToPublish("foo", "bar", ByteBuffer.allocate(1))
suspend fun putExample(q: Queue) {
    val offset = rnd.nextInt(60)

    val time = epoch() + offset
    q.add(ScheduledTask(rnd.nextLong(), time, tp))
}

fun main(args: Array<String>) = runBlocking {
    val q = Queue()
    val worker = queueWorker(q) { publish(it) }
    Runtime.getRuntime().addShutdownHook(Thread {
        q.close()
        worker.join()
        producer.close()

    })
    while(true) {
        putExample(q)
        Thread.sleep(1000)
    }
}
