package org.kinesis.core

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch

internal interface TaskState {

    enum class State {
        PENDING,    //注册后 还未被调度执行过
        WAITING,    //调度后依赖还未就绪 等待执行
        RUNNING,    //正在执行中
        COMPLETED,  //执行完成
        FAILED      //执行失败
    }

    val monitor: MutableStateFlow<State>
}


internal data class StatefulTask(
    val task: Task
) : TaskState {

    override val monitor: MutableStateFlow<TaskState.State> = MutableStateFlow(TaskState.State.PENDING).also {
        Kinesis.launch {
            it.collect { task ->
                Kinesis.debug("{}-{}", task.javaClass.name, it.value)
            }
        }
    }

    // Custom equality check
    override fun equals(other: Any?): Boolean {
        if (other !is StatefulTask) return false
        return task.javaClass == other.javaClass && monitor.value == other.monitor.value
    }

    // Ensure consistent hash code
    override fun hashCode(): Int {
        return task.hashCode()
    }

    override fun toString(): String {
        return "${task.javaClass.name}-${monitor.value}"
    }
}

