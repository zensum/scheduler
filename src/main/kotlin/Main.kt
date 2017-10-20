package scheduler

import franz.ProducerBuilder
import franz.WorkerBuilder
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.selects.selectUnbiased
import java.nio.ByteBuffer
import java.util.PriorityQueue
import se.zensum.idempotenceconnector.IdempotenceStore

import java.util.concurrent.TimeUnit

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

// The queue task represents the priority heap used for storing tasks,
// it allows waiting until the top most item is ripe for removing
class Queue {
    private val tasks = PriorityQueue<ScheduledTask>()
    private val wakeCh = Channel<Unit>()
    suspend fun add(t: ScheduledTask) = tasks.add(t).also { wakeCh.send(Unit) }
    fun takeBelow(l: Long): ScheduledTask? = tasks.peek().let {
        if (it != null && it.time < l) tasks.poll() else null
    }
    private fun sleepTime(): Long {
        val now = epoch()
        val sleepEnd = tasks.peek()?.time ?: Long.MAX_VALUE
        return sleepEnd - now
    }
    suspend fun waitAsRequired() {
        val sleepFor = sleepTime()
        if (sleepFor < 1) {
            return
        }
        selectUnbiased<Unit> {
            wakeCh.onReceive {
                waitAsRequired()
            }
            async {
                delay(sleepFor, TimeUnit.SECONDS)
            }
        }
    }
    fun closed() = wakeCh.isClosedForSend
}

suspend fun queueWorker(q: Queue, onItem: suspend (ScheduledTask) -> Unit) = async {
    while(!q.closed()) {
        val t = epoch()
        var item: ScheduledTask? = q.takeBelow(t)
        while (item != null) {
            onItem(item)
            item = q.takeBelow(t)
        }
        q.waitAsRequired()
    }
}

private val producer = ProducerBuilder.ofByteArray.create()
suspend fun publish(t: ScheduledTask) {
    val found = idem.contains(t.id.toString())
    if (found) {
        return
    }
    val d = t.data
    producer.send(d.topic, d.key, d.data.array())
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
fun main(args: Array<String>) = runBlocking {
    val q = Queue()
    queueWorker(q) { publish(it) }
    runConsumer(q)
}
