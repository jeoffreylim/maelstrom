package com.github.maelstrom

import java.util.concurrent.{CountDownLatch, ExecutorService, Executors}

import scala.collection.mutable.ListBuffer

/**
  * @author Jeoffrey Lim
  * @version 0.1
  */

class ProcessorRunner {
  final private val processors: ListBuffer[StreamProcessor[_, _]] = ListBuffer[StreamProcessor[_, _]]()

  final def addProcessor(processor: StreamProcessor[_, _]): ProcessorRunner = {
    processors += processor
    this
  }

  final def start() {
    val countDownLatch: CountDownLatch = new CountDownLatch(processors.size)
    val taskExecutor: ExecutorService = Executors.newFixedThreadPool(processors.size)

    for (p <- processors) taskExecutor.execute(p)

    try {
      countDownLatch.await()
    } catch {
      case e: InterruptedException =>
        Thread.currentThread.interrupt()
    }
  }

  final def stop() {
    for (p <- processors) p.stop()
  }
}