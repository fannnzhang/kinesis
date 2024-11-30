package org.kinesis.core

import kotlinx.coroutines.CoroutineDispatcher

interface Task {
    val dependencies: Set<Class<out Task>>

    val dispatcher: CoroutineDispatcher

    suspend fun run()

}