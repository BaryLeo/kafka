/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.utils.timer

import java.util.concurrent.{DelayQueue, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.{KafkaThread, Time}

trait Timer {
  /**
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
    * @param timerTask the task to add
    */
  def add(timerTask: TimerTask): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    * @param timeoutMs
    * @return whether or not any tasks were executed
    */
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
    * @return the number of tasks
    */
  def size: Int

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
    */
  def shutdown(): Unit
}

/**
 * 定时器
 *
 * @param executorName 用于taskExecutor内Thread的name
 * @param tickMs 时间精度(最底层时间轮一个单元格的时间跨度)
 * @param wheelSize 每层时间轮的单元格数量
 * @param startMs 创建时间
 */
@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1,
                  wheelSize: Int = 20,
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer {

  /**
   * timeout timer.
   *
   * 任务执行器, 已到期的任务会被submit到该线程池
   */
  private[this] val taskExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
    def newThread(runnable: Runnable): Thread =
      KafkaThread.nonDaemon("executor-"+executorName, runnable)
  })

  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  /**
   * 任务总数计数器,
   * 该对象被各级时间轮共用
   */
  private[this] val taskCounter = new AtomicInteger(0)
  /**
   * 最底层时间轮
   */
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  def add(timerTask: TimerTask): Unit = {
    readLock.lock()
    try {
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      readLock.unlock()
    }
  }

  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      if (!timerTaskEntry.cancelled)
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }

  private[this] val reinsert = (timerTaskEntry: TimerTaskEntry) => addTimerTaskEntry(timerTaskEntry)

  /**
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   *
   * 推进currentTime. 若产生了到期任务, 则交由taskExecutor来执行.
   *
   * @param timeoutMs 等待单元格到期的最大阻塞时间
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      writeLock.lock()
      try {
        // 拉到了bucket,说明TimerTaskList已到期
        while (bucket != null) {
          // 从下至上推进各级时间轮的currentTime
          timingWheel.advanceClock(bucket.getExpiration())
          /*
           * TimerTaskList到期,但其内的部分TimerTaskEntry可能没到期,
           * 将其中没到期的TimerTaskEntry拉出来重新由底层时间轮执行add操作,
           * 使得其落到了原时间轮的下一层.
           *
           * 至于已到期的TimerTaskEntry, 会在此次迭代中交由taskExecutor来执行
           * (见SystemTimer.addTimerTaskEntry方法)
           */
          bucket.flush(reinsert)
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  def size: Int = taskCounter.get

  override def shutdown() {
    taskExecutor.shutdown()
  }

}

