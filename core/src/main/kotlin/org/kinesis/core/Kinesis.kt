package org.kinesis.core

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.filterNot
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext


/**
 * Sealed class representing the main Kinesis engine.
 * Handles task management, dependency resolution, and task execution in a coroutine-based system.
 */
sealed class Kinesis : CoroutineScope, TaskManager, Task {

    init {
        // Initialize the task execution process.
        startLaunch()
    }

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
        Logger.getGlobal().info("Caught exception: $throwable")
    }

    /**
     * Flow of tasks to be executed, starting with the current Kinesis instance itself as a task.
     */
    private val taskProvider = MutableSharedFlow<StatefulTask>().apply {
        tryEmit(StatefulTask(this@Kinesis)) // Register Kinesis as an initial task.
    }

    /**
     * Registers a new task class, checking for duplicate or circular dependencies.
     * @param current The task class to register.
     * @return Result indicating success or failure of the registration.
     */
    override fun register(current: Class<Task>): Result<Boolean> {
        return kotlin.runCatching {
            // Check if the task is already registered.
            if (kinesisContext.contains(current)) {
                throw IllegalStateException("${current.simpleName} is already registered")
            }

            // Check for circular dependencies.
            if (kinesisContext.hasCircularDependency(current)) {
                throw IllegalStateException("${current.simpleName} has circular references")
            }

            // Create an instance of the task and emit it into the taskProvider.
            val instance = current.getDeclaredConstructor().newInstance()
            taskProvider.tryEmit(StatefulTask(instance))
        }
    }

    /**
     * The dependencies of the Kinesis task, which are empty by default.
     */
    override val dependencies: Set<Task> = emptySet()

    /**
     * The dispatcher for running tasks, defaulting to IO.
     */
    override val dispatcher: CoroutineDispatcher = Dispatchers.IO

    /**
     * The main execution logic for the Kinesis task itself.
     */
    override suspend fun run() {
        Logger.getGlobal().info("Kinesis start!")
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
            return node.task.dependencies.filterNot { kinesisContext.contains(it.javaClass) }.isEmpty()
        }

        // Launch a coroutine to process ready tasks.
        launch {
            taskProvider.filter(::isTaskReady).collect(::onRunTask)
        }

        // Launch a coroutine to process tasks that are not ready.
        launch {
            taskProvider.filterNot(::isTaskReady).collect(::onSuspendTask)
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
                }.onFailure {
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
        kotlin.runCatching {
            // Mark the task as suspended in the context.
            kinesisContext.suspendTask(node.task.javaClass)
        }.onFailure { throwable ->
            Logger.getGlobal().severe("Caught exception: $throwable") // Log any errors.
        }

        // Re-emit the task with its state updated to WAITING.
        taskProvider.tryEmit(node.apply { monitor.update { TaskState.State.WAITING } })
    }
}
