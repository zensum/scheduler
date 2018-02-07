package scheduler

import com.google.protobuf.ByteString
import franz.ProducerBuilder
import franz.WorkerBuilder
import kotlinx.coroutines.experimental.runBlocking
import mu.KotlinLogging
import se.zensum.idempotenceconnector.IdempotenceStore
import se.zensum.scheduler_proto.Scheduler.Task
import java.nio.charset.Charset
import java.time.Instant
import java.util.Random
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.DelayQueue
import java.util.concurrent.Delayed
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

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
    fun isRepeating() = task.intervalSeconds != 0 && task.expires != 0L
    fun isRemoved() = task.remove
    private fun shouldReschedule() = isRepeating() && !isExpired()
    private fun nextTime(): Long = time + task.intervalSeconds
    fun nextInstance(): ScheduledTask? =
        if (shouldReschedule())
            copy(time = nextTime())
        else null
    fun id() = task.id
    fun idAsString() = task.id.toString()
    fun data() = task.kmsg
}

fun readScheduledTask(x: ByteArray): ScheduledTask =
    ScheduledTask(Task.parseFrom(x)!!.also {
        if (it.expires == 0L && it.intervalSeconds != 0) {
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
        updateQueue(item, q)
    }
}}.also { it.start() }

private fun updateQueue(item: ScheduledTask, q: Queue) {
    val nextInstance: ScheduledTask? = item.nextInstance()
    if (nextInstance == null) {
        removed.remove(item.id())
    } else {
        q.add(nextInstance)
    }
}

private val producer = ProducerBuilder.ofByteArray.create()
suspend fun publish(t: ScheduledTask) {
    if (!t.isRepeating() && idem.contains(t.idAsString())) {
        return
    }
    val isRemoved = removed[t.id()]
    if (isRemoved != null && isRemoved) {
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
        "Published task ${t.idAsString()} delta = $delta repeating=${t.isRepeating()} expired=${t.isExpired()} took=$prodMS prodTook=$prodTook"
    }
}

var removed: MutableMap<Long, Boolean> = ConcurrentHashMap()

fun runConsumer(q: Queue) =
    WorkerBuilder.ofByteArray
        .subscribedTo(TOPIC)
        .groupId("scheduler")
        .handlePiped {
        it
            .map { it.value() }
            .map { readScheduledTask(it) }
            .advanceIf { !it.isExpired() }
            .execute {
                removed.put(it.id(), it.isRemoved())
                true
            }
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
val ids = rnd.nextLong()
suspend fun putExample(remove: Boolean) {
    //val offset = rnd.nextInt(60)
    val time = epoch() + 1

    val b = Task.newBuilder().apply {
        id = ids
        setTime(time)
        kmsg = tp
        expires = epoch() + 10
        intervalSeconds = 1
        this.remove = remove
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
    /*
    delay(10000)
    putExample(false)
    delay(6000)
    putExample( true)
    */
}
