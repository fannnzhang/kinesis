package org.kinesis.core

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.filterNot
import kotlinx.coroutines.flow.onSubscription
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KLogger
import mu.KotlinLogging
import java.util.logging.Logger
import java.util.logging.Level
import kotlin.coroutines.CoroutineContext


/**
 * Sealed class representing the main Kinesis engine.
 * Handles task management, dependency resolution, and task execution in a coroutine-based system.
 */
sealed class Kinesis : CoroutineScope, TaskManager, Task, KLogger by KotlinLogging.logger("Kinesis") {

    /**
     * Default singleton instance of Kinesis.
     */
    companion object Default : Kinesis()

    /**
     * Context for managing task dependencies and state.
     */
    private val kinesisContext = KinesisContext()

    /**
     * The coroutine context for Kinesis.
     * Includes a supervisor job, IO dispatcher, custom context for task management,
     * and a global exception handler.
     */
    override val coroutineContext: CoroutineContext = SupervisorJob() + Dispatchers.IO + kinesisContext + CoroutineExceptionHandler { context, throwable ->
        error("Caught exception: $throwable")
    }

    /**
     * Flow of tasks to be executed, starting with the current Kinesis instance itself as a task.
     */
    private val taskProvider = MutableSharedFlow<StatefulTask>(extraBufferCapacity = Int.MAX_VALUE)

    private val launcher: Deferred<Unit> = async {
        run()
    }.apply {
        invokeOnCompletion {
            debug("Kinesis self start complected!")
        }
    }


    private fun  hasCircularDependency(node: Class<out Task>, instance: Task? = null): Boolean {
        val task = instance ?: node.getDeclaredConstructor().newInstance()

        if (task.dependencies.isEmpty()) {
            kinesisContext.suspendTask(node)
            launch {
                launcher.await()
                taskProvider.emit(StatefulTask(task))
            }
            return false
        }

        if (!task.dependencies.contains(node)) {
            kinesisContext.suspendTask(node)
            if (!task.dependencies.any { hasCircularDependency(it, task) }) {
                launch {
                    launcher.await()
                    taskProvider.emit(StatefulTask(task))
                }
                return false
            }
        }

        return true
    }

    /**
     * Registers a new task class, checking for duplicate or circular dependencies.
     * @param current The task class to register.
     * @return Result indicating success or failure of the registration.
     */
    override fun register(current: Class<out Task>): Result<Boolean> {
        return kotlin.runCatching {
            // Check if the task is already registered.
            if (kinesisContext.contains(current)) {
                throw IllegalStateException("${current.simpleName} is already registered")
            }


            // Create an instance of the task and emit it into the taskProvider.
            val instance = current.getDeclaredConstructor().newInstance()

            // Check for circular dependencies.
            if (hasCircularDependency(current, instance)) {
                throw IllegalStateException("${current.simpleName} has circular references")
            }

            kinesisContext.contains(current)
        }
    }

    /**
     * The dependencies of the Kinesis task, which are empty by default.
     */
    override val dependencies: Set<Class<out Task>> = emptySet()

    /**
     * The dispatcher for running tasks, defaulting to IO.
     */
    override val dispatcher: CoroutineDispatcher = Dispatchers.Default

    /**
     * The main execution logic for the Kinesis task itself.
     */
    override suspend fun run() {
        info("Kinesis start!")
        startLaunch()
    }

    /**
     * Starts the launch processes for task execution and dependency resolution.
     */
    private fun startLaunch() {
        /**
         * Determines if a task is ready to be executed based on its dependencies.
         * @param node The task node to check.
         * @return True if the task is ready to run; false otherwise.
         */
        fun isTaskReady(node: StatefulTask): Boolean {
            // If there are no dependencies, the task is ready.
            if (node.task.dependencies.isEmpty()) {
                return true
            }

            // Check if all dependencies are present in the Kinesis context.
            return node.task.dependencies.filterNot { kinesisContext.contains(it) }.isEmpty()
        }

        launch {
            taskProvider.onSubscription {
                debug("Kinesis start subscription!")
            }.collect { task ->
                if (isTaskReady(task)) {
                    onRunTask(task)
                } else {
                    onSuspendTask(task)
                }
            }
        }.invokeOnCompletion {
            info("Kinesis start complected!")
        }

        launch {
            taskProvider.collect { task ->
                debug("{}-{}", task.task.javaClass.name, task.monitor.value)
            }
        }
    }

    /**
     * Handles the execution of a ready task.
     * @param node The task node to execute.
     */
    private fun onRunTask(node: StatefulTask) {
        node.monitor.update { TaskState.State.RUNNING } // Update the task state to RUNNING.

        launch(node.task.dispatcher) {
            // Add the task to the context and execute it asynchronously.
            kinesisContext.put(node.task.javaClass, async(node.task.dispatcher) {
                kotlin.runCatching {
                    node.task.run() // Run the task.
                }.onFailure { throwable ->
                    error("Caught task running exception: $throwable")
                    // Update state to FAILED if the task fails.
                    node.monitor.update { TaskState.State.FAILED }
                }.onSuccess {
                    // Update state to COMPLETED if the task succeeds.
                    node.monitor.update { TaskState.State.COMPLETED }
                }
            })
        }
    }

    /**
     * Handles tasks that are not ready for execution.
     * @param node The task node to process.
     */
    private fun onSuspendTask(node: StatefulTask) {
        // Re-emit the task with its state updated to WAITING.
        taskProvider.tryEmit(node.apply { monitor.update { TaskState.State.WAITING } })
    }
}
