package org.kinesis.core

interface TaskManager {
    fun register(current: Class<Task>) : Result<Boolean>
}