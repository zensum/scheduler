package scheduler

import com.google.protobuf.ByteString
import franz.ProducerBuilder
import franz.WorkerBuilder
import kotlinx.coroutines.experimental.runBlocking
import se.zensum.idempotenceconnector.IdempotenceStore
import java.util.Random
import java.util.concurrent.DelayQueue
import java.util.concurrent.Delayed
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import se.zensum.scheduler_proto.Scheduler.Task
import java.nio.charset.Charset
import mu.KotlinLogging
const val TOPIC = "scheduler"

// A scheduled task
data class ScheduledTask(private val task: Task): Delayed {
    override fun compareTo(other: Delayed?): Int =
        getDelay(TimeUnit.SECONDS)
            .compareTo(other!!.getDelay(TimeUnit.SECONDS))
    override fun getDelay(unit: TimeUnit?): Long =
        unit!!.convert(fromNow(task.time), TimeUnit.SECONDS)

    fun idAsString() = task.id.toString()
    fun data() = task.kmsg
}

fun readScheduledTask(x: ByteArray): ScheduledTask =
    ScheduledTask(Task.parseFrom(x)!!)

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
        onItem(q.take())
    }
}}.also { it.start() }

private val producer = ProducerBuilder.ofByteArray.create()
suspend fun publish(t: ScheduledTask) {
    val found = idem.contains(t.idAsString())
    if (found) {
        return
    }
    val d = t.data()
    producer.send(d.topic, d.key, d.body.toByteArray())
    idem.put(t.idAsString())
    logger.info {
        val delta = t.getDelay(TimeUnit.MILLISECONDS)
        "Published task ${t.idAsString()} delta = $delta"
    }
}

suspend fun runConsumer(q: Queue) =
    WorkerBuilder.ofByteArray
        .subscribedTo(TOPIC)
        .handlePiped {
        it
            .map { it.value() }
            .map { readScheduledTask(it) }
            .advanceIf { !idem.contains(it.idAsString()) } // Ugly longs
            .execute { q.add(it) }
            .success()
            .end()
        }.start()

val rnd = Random()
val tp = Task.KMsg.newBuilder().apply {
    body = ByteString.copyFrom("rhee", Charset.forName("UTF-8"))
    key = "normies"
    topic = "foo"
}.build()

suspend fun putExample(q: Queue) {
    val offset = rnd.nextInt(60)
    val time = epoch() + offset

    val b = Task.newBuilder().apply {
        id = rnd.nextLong()
        setTime(time)
        kmsg = tp
    }.build()
    logger.info("Publ example $b")
    producer.send(TOPIC, b.toByteArray())
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
