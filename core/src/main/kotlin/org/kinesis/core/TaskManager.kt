package org.kinesis.core

interface TaskManager {
    fun register(current: Class<out Task>) : Result<Boolean>
}