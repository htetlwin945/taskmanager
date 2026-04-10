/*
 * Copyright the State of the Netherlands
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/.
 */
package nl.aerius.taskmanager;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.aerius.taskmanager.TaskDispatcher.State;
import nl.aerius.taskmanager.domain.QueueConfig;
import nl.aerius.taskmanager.domain.Task;
import nl.aerius.taskmanager.domain.TaskConsumer;

/**
 * Test for {@link TaskDispatcher} class.
 */
class TaskDispatcherTest {

  private static final Logger LOG = LoggerFactory.getLogger(TaskDispatcherTest.class);

  private static final String WORKER_QUEUE_NAME_TEST = "TEST";
  private static ExecutorService executor;

  private TaskDispatcher dispatcher;
  private WorkerPool workerPool;
  private TaskConsumer taskConsumer;
  private MockWorkerProducer workerProducer;
  private MockAdaptorFactory factory;

  @BeforeEach
  void setUp() throws IOException {
    executor = Executors.newCachedThreadPool();
    final MockTaskScheduler scheduler = new MockTaskScheduler();
    workerProducer = new MockWorkerProducer();
    workerPool = new WorkerPool(WORKER_QUEUE_NAME_TEST, workerProducer, scheduler);
    dispatcher = new TaskDispatcher(WORKER_QUEUE_NAME_TEST, scheduler, workerPool);
    factory = new MockAdaptorFactory();
    taskConsumer = new TaskConsumerImpl(executor, new QueueConfig("testqueue", false, false, -1, null), dispatcher, factory);
  }

  @AfterEach
  void after() throws InterruptedException {
    dispatcher.shutdown();
    executor.shutdownNow();
    executor.awaitTermination(10, TimeUnit.MILLISECONDS);
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.SECONDS)
  void testNoFreeWorkers() {
    // Add Worker which will unlock
    workerPool.onNumberOfWorkersUpdate(1, 0, 0);
    executor.execute(dispatcher);
    await().until(() -> dispatcher.getState() == State.WAIT_FOR_TASK);
    // Remove worker, 1 worker locked but at this point no actual workers available.
    workerPool.onNumberOfWorkersUpdate(0, 0, 0);
    // Send task, should get NoFreeWorkersException in dispatcher.
    forwardTaskAsync(createTask(), null);
    // Dispatcher should go back to wait for worker to become available.
    await().until(() -> dispatcher.getState() == State.WAIT_FOR_WORKER);
    assertEquals(0, workerPool.getReportedWorkerSize(), "WorkerPool should be empty");
    workerPool.onNumberOfWorkersUpdate(1, 0, 0);
    assertEquals(1, workerPool.getReportedWorkerSize(), "WorkerPool should have 1 running");
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.SECONDS)
  void testForwardTest() {
    final Task task = createTask();
    final Future<?> future = forwardTaskAsync(task, null);
    executor.execute(dispatcher);
    workerPool.onNumberOfWorkersUpdate(1, 0, 0); //add worker which will unlock
    await().until(() -> dispatcher.getState() == State.WAIT_FOR_WORKER);
    await().until(future::isDone);
    assertFalse(future.isCancelled(), "Taskconsumer must be unlocked at this point without error");
  }

  @Disabled("TaskAlreadySendexception error willl not be thrown")
  @Test
  @Timeout(value = 3, unit = TimeUnit.SECONDS)
  void testForwardDuplicateTask() {
    final Task task = createTask();
    executor.execute(dispatcher);
    final Future<?> future = forwardTaskAsync(task, null);
    await().until(() -> dispatcher.getState() == State.WAIT_FOR_WORKER);
    workerPool.onNumberOfWorkersUpdate(2, 0, 0); //add worker which will unlock
    // Now force the issue.
    assertSame(TaskDispatcher.State.WAIT_FOR_TASK, dispatcher.getState(), "Taskdispatcher must be waiting for task");
    // Forwarding same Task object, so same id.
    forwardTaskAsync(task, future);
    await().until(() -> factory.getMockTaskMessageHandler().getAbortedMessage() == null);
    // Now test with a non-duplicate Task.
    forwardTaskAsync(createTask(), future);
    await().until(() -> dispatcher.getState() == State.WAIT_FOR_WORKER);
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.SECONDS)
  void testExceptionDuringForward() {
    workerProducer.setShutdownExceptionOnForward(true);
    final Task task = createTask();
    executor.execute(dispatcher);
    final Future<?> future = forwardTaskAsync(task, null);
    await().until(() -> dispatcher.getState() == State.WAIT_FOR_WORKER);
    // Now open up a worker
    workerPool.onNumberOfWorkersUpdate(1, 0, 0);
    // At this point the exception should be thrown. This could be the case when rabbitmq connection is lost for a second.
    // Wait for it to be unlocked again
    await().until(() -> dispatcher.getState() == State.WAIT_FOR_TASK);
    //simulate workerpool being reset
    workerPool.onNumberOfWorkersUpdate(0, 0, 0);
    //now stop throwing exception to indicate connection is restored again
    workerProducer.setShutdownExceptionOnForward(false);
    //simulate connection being restored by first forwarding task again
    forwardTaskAsync(task, future);
    await().until(() -> dispatcher.getState() == State.WAIT_FOR_WORKER);
    //now simulate the worker being back
    workerPool.onNumberOfWorkersUpdate(1, 0, 0);
    //should now be unlocked, but waiting for worker to be done
    await().until(() -> dispatcher.getState() == State.WAIT_FOR_WORKER);
    workerPool.onWorkerFinished(task.getId(), Map.of());
    //should now again be ready to be used
    await().until(() -> dispatcher.getState() == State.WAIT_FOR_TASK);
    // Check if we can send a task again.
    forwardTaskAsync(createTask(), future);
    await().until(() -> dispatcher.getState() == State.WAIT_FOR_WORKER);
    // If state changed at this point we're reasonably sure the dispatcher is still functional.
  }

  private Future<?> forwardTaskAsync(final Task task, final Future<?> previous) {
    return executor.submit(() -> {
      try {
        if (previous != null) {
          previous.get();
        }
        dispatcher.forwardTask(task);
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Exception in dispatcher forwardTask", e);
      }
    });
  }

  private Task createTask() {
    return new MockTask(taskConsumer);
  }
}
