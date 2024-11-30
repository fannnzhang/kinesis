package org.kinesis.core

import kotlinx.coroutines.Deferred
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext

/**
 * KinesisContext: A CoroutineContext.Element for managing task execution and dependency tracking.
 */
class KinesisContext : CoroutineContext.Element {

    companion object Key : CoroutineContext.Key<KinesisContext>

    override val key: CoroutineContext.Key<*> get() = Key

    // Record of completed tasks and their results
    private val taskHistoryRecorder: ConcurrentHashMap<Class<out Task>, Deferred<Result<*>>> = ConcurrentHashMap()

    // Record of tasks currently in a suspended state
    private val suspendingTaskRecorder = ConcurrentHashMap.newKeySet<Class<out Task>>()

    // Dependency graph to track task dependencies
    private val dependencyGraph: ConcurrentHashMap<Class<out Task>, Set<Class<out Task>>> = ConcurrentHashMap()

//    /**
//     * Check if a task has circular dependencies.
//     *
//     * @param key The class of the task to check.
//     * @return True if a circular dependency is detected, otherwise false.
//     */
//    internal fun hasCircularDependency(key: Class<out Task>): Boolean {
//        val visited = ConcurrentHashMap.newKeySet<Class<out Task>>() // Thread-safe visited set
//        val stack = ConcurrentHashMap.newKeySet<Class<out Task>>() // Thread-safe recursion stack
//
//        fun dfs(node: Class<out Task>): Boolean {
//            if (stack.contains(node)) return true // Found a cycle
//            if (visited.contains(node)) return false
//
//            visited.add(node)
//            stack.add(node)
//
//            val dependencies = dependencyGraph[node] ?: emptySet()
//
//            if (dependencies.contains(node)) {
//                return true
//            }
//
//            for (dependency in dependencies) {
//                if (dfs(dependency)) return true
//            }
//
//            stack.remove(node)
//            return false
//        }
//
//        return dfs(key)
//    }

    /**
     * Add task dependencies to the dependency graph.
     *
     * @param task The task to register dependencies for.
     */
    internal fun addTaskDependencies(task: Task) {
        Kinesis.debug("DependencyGraph injected {} with dependencies {}", task.javaClass.name, task.dependencies.joinToString(","))
        dependencyGraph[task.javaClass] = task.dependencies.map { it}.toSet()
    }

    /**
     * Check if a task is already completed or being processed.
     *
     * @param type The class of the task to check.
     * @return True if the task is found, otherwise false.
     */
    internal fun contains(type: Class<out Task>): Boolean {
        if (taskHistoryRecorder.containsKey(type)) {
            return true
        }

        if (suspendingTaskRecorder.contains(type)) {
            return synchronized(suspendingTaskRecorder) {
                suspendingTaskRecorder.contains(type)
            }
        }

        return false
    }

    /**
     * Record the result of a completed task.
     *
     * @param key The class of the task.
     * @param job The result of the task as a Deferred object.
     */
    internal fun put(key: Class<Task>, job: Deferred<Result<*>>) {
        if (taskHistoryRecorder.containsKey(key)) {
            throw IllegalStateException("${key.simpleName} has already been recorded in KinesisContext")
        }

        synchronized(suspendingTaskRecorder) {
            suspendingTaskRecorder.remove(key)
        }

        taskHistoryRecorder[key] = job
    }

    /**
     * Mark a task as suspended.
     *
     * @param key The class of the task to suspend.
     */
    internal fun suspendTask(key: Class<out Task>) {
        if (suspendingTaskRecorder.contains(key)) {
            throw IllegalStateException("${key.simpleName} is already marked as suspended in KinesisContext")
        }

        synchronized(suspendingTaskRecorder) {
            suspendingTaskRecorder.add(key)
        }
    }

    /**
     * Check if a task is suspended.
     *
     * @param key The class of the task to check.
     * @return True if the task is suspended, otherwise false.
     */
    internal fun isSuspended(key: Class<Task>): Boolean {
        return suspendingTaskRecorder.contains(key)
    }
}
