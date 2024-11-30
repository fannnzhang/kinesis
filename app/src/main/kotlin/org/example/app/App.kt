/*
 * This source file was generated by the Gradle 'init' task
 */
package org.example.app

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import org.kinesis.core.Kinesis
import kotlinx.coroutines.runBlocking
import org.kinesis.core.Task

var executed = false
class ExecutableTask : Task {
    override val dependencies: Set<Class<out Task>> = emptySet()
    override val dispatcher: CoroutineDispatcher = Dispatchers.Default
    override suspend fun run() {
        executed = true
        Kinesis.debug("ExecutableTask executed $executed")
    }
}

class TaskA : Task {
    override val dependencies: Set<Class<out Task>> by lazy { setOf(TaskB::class.java) }
    override val dispatcher: CoroutineDispatcher = Dispatchers.Default
    override suspend fun run() {}
}

class TaskB : Task {
    override val dependencies: Set<Class<out Task>> by lazy { setOf(TaskC::class.java) }
    override val dispatcher: CoroutineDispatcher = Dispatchers.Default
    override suspend fun run() {}
}

class TaskC : Task {
    override val dependencies: Set<Class<out Task>> by lazy { setOf(TaskA::class.java) }
    override val dispatcher: CoroutineDispatcher = Dispatchers.Default
    override suspend fun run() {}
}

class TaskD : Task {
    override val dependencies: Set<Class<out Task>> by lazy { emptySet() }
    override val dispatcher: CoroutineDispatcher = Dispatchers.Default
    override suspend fun run() {
        throw RuntimeException("Test TaskD")
    }
}


fun main() {
    runBlocking {
        //Test circular references
        Kinesis.register(TaskA::class.java).exceptionOrNull()?.printStackTrace()
        Kinesis.register(TaskB::class.java).exceptionOrNull()?.printStackTrace()
        Kinesis.register(TaskC::class.java).exceptionOrNull()?.printStackTrace()

        // Test Task have exception
        Kinesis.register(TaskD::class.java).exceptionOrNull()?.printStackTrace()

        Kinesis.register(ExecutableTask::class.java).exceptionOrNull()?.printStackTrace()
        delay(10000)
    }


}
