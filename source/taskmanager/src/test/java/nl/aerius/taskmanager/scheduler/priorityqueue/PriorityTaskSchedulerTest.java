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
package nl.aerius.taskmanager.scheduler.priorityqueue;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.aerius.taskmanager.MockAdaptorFactory;
import nl.aerius.taskmanager.MockTask;
import nl.aerius.taskmanager.TaskConsumerImpl;
import nl.aerius.taskmanager.domain.ForwardTaskHandler;
import nl.aerius.taskmanager.domain.Message;
import nl.aerius.taskmanager.domain.PriorityTaskQueue;
import nl.aerius.taskmanager.domain.PriorityTaskSchedule;
import nl.aerius.taskmanager.domain.QueueConfig;
import nl.aerius.taskmanager.domain.Task;
import nl.aerius.taskmanager.domain.TaskConsumer;

/**
 * Test class for {@link PriorityTaskScheduler}.
 */
class PriorityTaskSchedulerTest {

  private static final Logger LOG = LoggerFactory.getLogger(PriorityTaskSchedulerTest.class);

  private static final String QUEUE1 = "queue1";
  private static final String QUEUE2 = "queue2";
  private static final String QUEUE3 = "queue3";
  private static final double TEST_CAPACITY = 0.7;
  private static final PriorityTaskSchedulerFactory FACTORY = new PriorityTaskSchedulerFactory();

  private TaskConsumer taskConsumer1;
  private TaskConsumer taskConsumer2;
  private Task task1;
  private Task task2a;
  private Task task2b;
  private Task task3;
  private PriorityTaskSchedule configuration;
  private PriorityTaskScheduler scheduler;

  @BeforeEach
  void setUp() throws IOException {
    taskConsumer1 = createMockTaskConsumer(QUEUE1);
    taskConsumer2 = createMockTaskConsumer(QUEUE2);
    final TaskConsumer taskConsumer3 = createMockTaskConsumer(QUEUE3);
    configuration = new PriorityTaskSchedule();
    configuration.setWorkerQueueName("TEST");
    final PriorityTaskQueue tc1 = new PriorityTaskQueue(QUEUE1, 0, TEST_CAPACITY);
    final PriorityTaskQueue tc2 = new PriorityTaskQueue(QUEUE2, 1, TEST_CAPACITY);
    final PriorityTaskQueue tc3 = new PriorityTaskQueue(QUEUE3, 1, TEST_CAPACITY);
    configuration.getQueues().add(tc1);
    configuration.getQueues().add(tc2);
    configuration.getQueues().add(tc3);
    scheduler = createScheduler(0);
    task1 = createTask(taskConsumer1, "1");
    task2a = createTask(taskConsumer2, "2a");
    task2b = createTask(taskConsumer2, "2b");
    task3 = createTask(taskConsumer3, "3");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  void testCompare() throws InterruptedException {
    assertTrue(compare(task1, task2a, 1), "Compare Ok");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  void testCompareReverse() throws InterruptedException {
    assertTrue(compare(task2a, task1, -1), "Compare reserve Ok");
  }

  private boolean compare(final Task taskA, final Task taskB, final int returnResult) throws InterruptedException {
    scheduler.onWorkerPoolSizeChange(3);
    assertEquals(returnResult, scheduler.compare(taskA, taskB),
        "Task with higher priority and no tasks running should be returned");
    scheduler.addTask(task2a);
    assertSame(task2a, scheduler.getNextTask(), "Should get task2a back.");
    assertEquals(-returnResult, scheduler.compare(taskA, taskB),
        "Task with lower priority, but no task run yet should be done returned");
    scheduler.addTask(task1);
    assertSame(task1, scheduler.getNextTask(), "Should get task1 back.");
    assertEquals(returnResult, scheduler.compare(taskA, taskB),
        "Task with higher priority should be returned when both with tasks running ");
    return true;
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  void testCompareSame() throws InterruptedException {
    assertTrue(compareSame(task2a, task3, -1), "Compare same Ok");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.SECONDS)
  void testCompareSameReverse() throws InterruptedException {
    assertTrue(compareSame(task3, task2a, 1), "Compare same reserve Ok");
  }

  private boolean compareSame(final Task taskA, final Task taskB, final int returnResult) throws InterruptedException {
    scheduler.onWorkerPoolSizeChange(4);
    assertEquals(0, scheduler.compare(taskA, taskB),
        "Task with same priority and no tasks running return 0");
    scheduler.addTask(task2a);
    assertSame(task2a, scheduler.getNextTask(), "Should get task2a back.");
    assertEquals(returnResult, scheduler.compare(taskA, taskB),
        "Task with same priority, but no task run yet should be done returned");
    scheduler.addTask(task3);
    assertSame(task3, scheduler.getNextTask(), "Should get task3 back.");
    assertEquals(0, scheduler.compare(taskA, taskB),
        "Task with same priority should return 0");
    scheduler.addTask(task2b);
    assertSame(task2b, scheduler.getNextTask(), "Should get task2b back.");
    assertEquals(-returnResult, scheduler.compare(taskA, taskB),
        "Task with same priority, but one with more running, should run with less running");
    return true;
  }

  @Test
  @Timeout(7000)
  void testGetTaskWith1WorkerAvailable() throws InterruptedException, ExecutionException {
    scheduler.onWorkerPoolSizeChange(1);
    final Task task = createTask(taskConsumer1, "1"); //add task with priority 0.
    scheduler.addTask(task);
    final AtomicInteger chkCounter = new AtomicInteger();
    final Future<Task> receivedTask = waitForTask(task, chkCounter);
    await().atMost(1, TimeUnit.SECONDS).until(receivedTask::isDone);
    assertNotNull(receivedTask.get(), "Received task");
    assertEquals(1, chkCounter.intValue(), "Counter should be 1 when only one slot available");
  }

  /**
   * Test if getNextTask correctly gets new task.
   * If only 1 slot available and the next task is a low priority of the queue on which already another process is running,
   * it should not be run until that more than one or the task of that queue is finished.
   */
  @Test
  @Timeout(value = 7, unit = TimeUnit.SECONDS)
  void testGetTask() throws InterruptedException, ExecutionException {
    scheduler.onWorkerPoolSizeChange(2);
    final Task task1a = createTask(taskConsumer1, "1a");
    scheduler.addTask(task1a);
    final Task task1b = createTask(taskConsumer1, "1b");
    scheduler.addTask(task1b);
    scheduler.addTask(task2a);
    assertSame(task2a, scheduler.getNextTask(), "Should get task2a back.");
    assertSame(task1a, scheduler.getNextTask(), "Should get task1a back.");
    final AtomicInteger chkCounter = new AtomicInteger();
    final Future<Task> receivedTask = waitForTask(task1b, chkCounter);
    await().pollDelay(1, TimeUnit.SECONDS).until(() -> true);
    assertEquals(0, chkCounter.intValue(), "Counter should still be zero");
    assertFalse(receivedTask.isDone(), "Should not be done yet");
    scheduler.onTaskFinished(task2a.getTaskRecord());
    await().pollDelay(1, TimeUnit.SECONDS).until(() -> true);
    // task2a finished, but task1b may still not be executed, because only 1 slot available.
    assertEquals(0, chkCounter.intValue(), "Counter should still be zero when 1 slot priorty available");
    assertFalse(receivedTask.isDone(), "Should not be done yet");
    scheduler.onTaskFinished(task1a.getTaskRecord());
    await().atMost(1, TimeUnit.SECONDS).until(receivedTask::isDone);
    assertNotNull(receivedTask.get(), "Received task");
    // task1a finished, now task1b may be executed.
    assertEquals(1, chkCounter.intValue(), "Counter should still be 1");
  }

  /**
   * Test if getNextTask correctly gets new task in case of a big worker pool.
   * If capacity is reached for a task, it should not run unless a task of the same queue is returned as finished.
   * In the meanwhile, other tasks can start/finish (as long as there is a capacity for those tasks).
   *
   * This test also tests the maxWorkersAvailable setting.
   *
   * @param maxWorkersAvailable the configured maximum number of workers available, 0 means not set
   * @param nrOfTasksToTake number of tasks to initial take to reach 70% of total number of available workers
   */
  @ParameterizedTest
  @CsvSource({
      // No max worker available set. With 10 actual workers available it should take 70% of that, which is 7
      "0,7",
      // Max workers available is 20. With 10 actual workers it should look at the potential 20 numbers, and take 70% of that, which is 14.
      "20,14"})
  @Timeout(value = 7, unit = TimeUnit.SECONDS)
  void testGetTaskBigPool(final int maxWorkersAvailable, final int nrOfTasksToTake) throws InterruptedException, ExecutionException {
    scheduler = createScheduler(maxWorkersAvailable);
    scheduler.onWorkerPoolSizeChange(10);
    final List<Task> tasks = new ArrayList<>();
    final List<Task> sendTasks = new ArrayList<>();
    // Put 20 tasks in the queue to be picked up.
    for (int i = 0; i < 20; i++) {
      final Task task = createTask(taskConsumer2, "1");
      scheduler.addTask(task);
      tasks.add(task);
    }
    // It should at least be able to get the given nr of nrOfTasksToTake
    for (int i = 0; i < nrOfTasksToTake; i++) {
      sendTasks.add(scheduler.getNextTask());
    }
    scheduler.addTask(task1);
    assertSame(task1, scheduler.getNextTask(), "Should still get task 1 because other tasks have reach the limit");
    final Task task1b = createTask(taskConsumer1, "1b");
    scheduler.addTask(task1b);
    assertSame(task1b, scheduler.getNextTask(), "Should still get task 1b");
    final AtomicInteger chkCounter = new AtomicInteger();
    final Future<Task> receivedTask = waitForTask(null, chkCounter);
    await().pollDelay(1, TimeUnit.SECONDS).until(() -> true);
    assertEquals(0, chkCounter.intValue(), "Counter should still be zero");
    assertFalse(receivedTask.isDone(), "Should not be done yet");
    scheduler.onTaskFinished(task1.getTaskRecord());
    scheduler.onTaskFinished(task1b.getTaskRecord());
    await().pollDelay(1, TimeUnit.SECONDS).until(() -> true);
    // task1's are finished, but tasks on 2 may still not be executed, because not enough slots available.
    assertEquals(0, chkCounter.intValue(), "Counter should still be zero when capacity not reached");
    assertFalse(receivedTask.isDone(), "Should not be done yet");
    scheduler.onTaskFinished(sendTasks.get(0).getTaskRecord());
    await().atMost(1, TimeUnit.SECONDS).until(receivedTask::isDone);
    assertNotNull(receivedTask.get(), "Received task");
    // One of the task2's is now finished, another task2 may be executed.
    assertEquals(1, chkCounter.intValue(), "Counter should increase because there is enough capacity now");
  }

  /**
   * If 2 workers available and 1 task (from queue 2) already handled by worker then when 2 tasks are added, one for queue 2 and one for queue 3 then
   * the task of queue 3 should be selected.
   * @throws InterruptedException
   */
  @Test
  @Timeout(value = 1, unit = TimeUnit.SECONDS)
  void testCompare2Workers() throws InterruptedException {
    scheduler.onWorkerPoolSizeChange(2);
    scheduler.addTask(task2a);
    scheduler.getNextTask();
    assertEquals(1, scheduler.compare(task2b, task3), "Scheduler should prefer task3");
    scheduler.addTask(task2b);
    scheduler.addTask(task3);
    assertSame(task3, scheduler.getNextTask(), "Scheduler should prefer task3");
  }

  @Test
  @Timeout(value = 1, unit = TimeUnit.SECONDS)
  void testReset() throws InterruptedException {
    scheduler.onWorkerPoolSizeChange(2);
    scheduler.addTask(task2a);
    scheduler.getNextTask();
    assertEquals(1, scheduler.compare(task2b, task3), "Scheduler should prefer task3");
    scheduler.reset();
    assertEquals(0, scheduler.compare(task2b, task3), "After reset Scheduler should not have a preference because all are on the same level");
  }

  private Future<Task> waitForTask(final Task task, final AtomicInteger chkCounter) {
    final ExecutorService es = Executors.newCachedThreadPool();
    final Future<Task> receivedTask = es.submit(new Callable<Task>() {
      @Override
      public Task call() throws Exception {
        Task result = null;
        try {
          result = scheduler.getNextTask();
        } catch (final InterruptedException e) {
          LOG.error("InterruptedException when waiting for next task", e);
        }
        if (task == null) {
          assertNotNull(result, "Should get any task back");
        } else {
          assertSame(task, result, "Should get task back.");
        }
        chkCounter.incrementAndGet();
        return result;
      }
    });
    es.shutdown();
    return receivedTask;
  }

  private TaskConsumer createMockTaskConsumer(final String taskQueueName) throws IOException {
    return new TaskConsumerImpl(mock(ExecutorService.class), new QueueConfig(taskQueueName, false, false, -1, null), mock(ForwardTaskHandler.class),
        new MockAdaptorFactory()) {
      @Override
      public void messageDelivered(final Message message) {
        //no-op.
      }
    };
  }

  private PriorityTaskScheduler createScheduler(final int maxWorkerAvailable) {
    final PriorityTaskScheduler scheduler = (PriorityTaskScheduler) FACTORY
        .createScheduler(new QueueConfig(QUEUE1, false, true, maxWorkerAvailable, null));

    configuration.getQueues().forEach(scheduler::updateQueue);
    return scheduler;
  }

  private static Task createTask(final TaskConsumer tc, final String messageId) {
    return new MockTask(tc, messageId);
  }
}
