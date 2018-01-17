package scheduler

import com.google.protobuf.ByteString
import franz.ProducerBuilder
import franz.WorkerBuilder
import kotlinx.coroutines.experimental.runBlocking
import mu.KLogging
import se.zensum.idempotenceconnector.IdempotenceStore
import java.util.Random
import java.util.concurrent.DelayQueue
import java.util.concurrent.Delayed
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import se.zensum.scheduler_proto.Scheduler.Task
import java.nio.charset.Charset
import mu.KotlinLogging
import java.time.Instant

const val TOPIC = "scheduler"

private val logger = KotlinLogging.logger("scheduler")

// A scheduled task
data class ScheduledTask(private val task: Task, private val time: Long): Delayed {
    constructor(task: Task): this(task, task.time) {}

    override fun compareTo(other: Delayed?): Int =
        getDelay(TimeUnit.SECONDS)
            .compareTo(other!!.getDelay(TimeUnit.SECONDS))

    override fun getDelay(unit: TimeUnit?): Long =
        unit!!.convert(fromNow(time), TimeUnit.SECONDS)
    fun isExpired(): Boolean = task.expires != 0L && epoch() > task.expires
    fun isRepeating() = task.interval != 0 && task.expires != 0L
    private fun shouldReschedule() = isRepeating() && !isExpired()
    private fun nextTime(): Long = time + task.interval
    fun nextInstance(): ScheduledTask? =
        if (shouldReschedule())
            copy(time = nextTime())
        else null

    fun idAsString() = task.id.toString()
    fun data() = task.kmsg
}

fun readScheduledTask(x: ByteArray): ScheduledTask =
    ScheduledTask(Task.parseFrom(x)!!.also {
        if (it.expires == 0L && it.interval != 0) {
            logger.warn("WARNING: messages w/ intervals must an expiry. id=${it.id}")
        }
    })

val idem = IdempotenceStore("scheduler")

fun epoch() = Instant.now().epochSecond
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
        val item = q.take()
        onItem(item)
        item.nextInstance()?.let {
            q.add(it)
        }
    }
}}.also { it.start() }

private val producer = ProducerBuilder.ofByteArray.create()
suspend fun publish(t: ScheduledTask) {
    if (!t.isRepeating() && idem.contains(t.idAsString())) {
        return
    }
    val d = t.data()
    val prodMS = System.currentTimeMillis()
    producer.send(d.topic, d.key, d.body.toByteArray())
    val afterMS = System.currentTimeMillis()

    val prodTook = afterMS - prodMS

    idem.put(t.idAsString())
    logger.info {
        val delta = t.getDelay(TimeUnit.MILLISECONDS)
        "Published task ${t.idAsString()} delta = $delta repeating=${t.isRepeating()} expired=${t.isExpired()} took=${prodMS}"
    }
}

fun runConsumer(q: Queue) =
    WorkerBuilder.ofByteArray
        .subscribedTo(TOPIC)
        .groupId("scheduler")
        .handlePiped {
        it
            .map { it.value() }
            .map { readScheduledTask(it) }
            .advanceIf { !it.isExpired() }
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
    //val offset = rnd.nextInt(60)
    val time = epoch() + 1

    val b = Task.newBuilder().apply {
        id = rnd.nextLong()
        setTime(time)
        kmsg = tp
        expires = epoch() + 30
        interval = 1
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
    runConsumer(q)
    var i = 3
    while(i-- > 0) {
        putExample(q)
        Thread.sleep(1000)
    }
}
