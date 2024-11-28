package org.kinesis.core

import kotlinx.coroutines.CoroutineDispatcher

interface Task {
    val dependencies: Set<Task>

    val dispatcher: CoroutineDispatcher

    suspend fun run()

}