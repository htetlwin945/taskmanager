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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.aerius.taskmanager.adaptor.AdaptorFactory;
import nl.aerius.taskmanager.adaptor.WorkerProducer;
import nl.aerius.taskmanager.adaptor.WorkerSizeObserver;
import nl.aerius.taskmanager.adaptor.WorkerSizeProviderProxy;
import nl.aerius.taskmanager.domain.QueueConfig;
import nl.aerius.taskmanager.domain.TaskConsumer;
import nl.aerius.taskmanager.domain.TaskQueue;
import nl.aerius.taskmanager.domain.TaskSchedule;
import nl.aerius.taskmanager.metrics.OpenTelemetryMetrics;
import nl.aerius.taskmanager.metrics.PerformanceMetricsReporter;
import nl.aerius.taskmanager.metrics.RabbitMQUsageMetricsProvider;
import nl.aerius.taskmanager.metrics.TaskManagerMetricsRegister;
import nl.aerius.taskmanager.metrics.TaskManagerUsageMetricsProvider;
import nl.aerius.taskmanager.metrics.TaskManagerUsageMetricsWrapper;
import nl.aerius.taskmanager.scheduler.TaskScheduler;
import nl.aerius.taskmanager.scheduler.TaskScheduler.TaskSchedulerFactory;

/**
 * Main task manager class, manages all schedulers.
 */
class TaskManager<T extends TaskQueue, S extends TaskSchedule<T>> {

  private static final Logger LOG = LoggerFactory.getLogger(TaskManager.class);

  private final ExecutorService executorService;
  private final ScheduledExecutorService scheduledExecutorService;
  private final AdaptorFactory factory;
  private final TaskSchedulerFactory<T, S> schedulerFactory;
  private final WorkerSizeProviderProxy workerSizeObserverProxy;
  private final Map<String, TaskScheduleBucket> buckets = new HashMap<>();
  private final TaskManagerUsageMetricsWrapper taskManagerMetrics;

  public TaskManager(final ExecutorService executorService, final ScheduledExecutorService scheduledExecutorService, final AdaptorFactory factory,
      final TaskSchedulerFactory<T, S> schedulerFactory, final WorkerSizeProviderProxy workerSizeObserverProxy) {
    this.executorService = executorService;
    this.scheduledExecutorService = scheduledExecutorService;
    this.factory = factory;
    this.schedulerFactory = schedulerFactory;
    this.workerSizeObserverProxy = workerSizeObserverProxy;
    this.taskManagerMetrics = new TaskManagerUsageMetricsWrapper(OpenTelemetryMetrics.METER);
  }

  /**
   * Add or Update a new task scheduler.
   *
   * @param schedule scheduler configuration
   * @throws InterruptedException
   */
  public void updateTaskScheduler(final TaskSchedule<T> schedule) throws InterruptedException {
    // Set up scheduler with worker pool
    final String workerQueueName = schedule.getWorkerQueueName();
    final QueueConfig workerQueueConfig = new QueueConfig(workerQueueName, schedule.isDurable(), schedule.isEagerFetch(),
        schedule.getMaxWorkersAvailable(), schedule.getQueueType());

    if (!buckets.containsKey(workerQueueName)) {
      LOG.info("Added scheduler for worker queue {}", schedule);
      buckets.put(workerQueueName, new TaskScheduleBucket(workerQueueConfig));
    }
    final TaskScheduleBucket taskScheduleBucket = buckets.get(workerQueueName);

    taskScheduleBucket.updateQueues(schedule.getQueues(), workerQueueConfig);
  }

  public boolean isRunning(final String queue) {
    return Optional.ofNullable(buckets.get(queue)).map(TaskScheduleBucket::isRunning).orElse(false);
  }

  /**
   * Removed the scheduler for the given worker type.
   *
   * @param workerQueueName queueName
   */
  public void removeTaskScheduler(final String workerQueueName) {
    LOG.info("Removed schedule for worker queue {}", workerQueueName);
    final TaskScheduleBucket bucket = buckets.get(workerQueueName);

    if (bucket != null) {
      bucket.shutdown();
      buckets.remove(workerQueueName);
    }
  }

  /**
   * Shuts down all schedulers and consumers.
   */
  public void shutdown() {
    buckets.forEach((k, v) -> v.shutdown());
    buckets.clear();
    taskManagerMetrics.close();
  }

  /**
   * Returns the {@link TaskScheduleBucket}. Intended to be used in tests.
   *
   * @param queueName queue to get the bucket for
   * @return the bucket for the given queue name
   */
  TaskScheduleBucket getTaskScheduleBucket(final String queueName) {
    return buckets.get(queueName);
  }

  class TaskScheduleBucket {
    private final TaskDispatcher dispatcher;
    private final WorkerProducer workerProducer;
    private final Map<String, TaskConsumer> taskConsumers = new HashMap<>();
    private final TaskScheduler<T> taskScheduler;
    private final String workerQueueName;

    public TaskScheduleBucket(final QueueConfig queueConfig) throws InterruptedException {
      this.workerQueueName = queueConfig.queueName();
      final QueueWatchDog watchDog = new QueueWatchDog(workerQueueName);
      final StartupGuard startupGuard = new StartupGuard();

      taskScheduler = schedulerFactory.createScheduler(queueConfig);
      workerProducer = factory.createWorkerProducer(queueConfig);
      final WorkerPool workerPool = new WorkerPool(workerQueueName, workerProducer, taskScheduler);
      final TaskManagerUsageMetricsProvider taskManagerUsageMetrics = new TaskManagerUsageMetricsProvider(workerQueueName);
      final TaskManagerMetricsRegister taskManagerMetricsRegister = new TaskManagerMetricsRegister(taskManagerUsageMetrics, startupGuard);
      final PerformanceMetricsReporter reporter = new PerformanceMetricsReporter(scheduledExecutorService, queueConfig.queueName(),
          OpenTelemetryMetrics.METER);

      watchDog.addQueueWatchDogListener(workerPool);
      watchDog.addQueueWatchDogListener(taskScheduler);
      watchDog.addQueueWatchDogListener(reporter);
      watchDog.addQueueWatchDogListener(taskManagerMetricsRegister);

      workerProducer.addWorkerProducerHandler(reporter);
      workerProducer.addWorkerProducerHandler(taskManagerMetricsRegister);
      workerProducer.addWorkerProducerHandler(watchDog);

      final RabbitMQUsageMetricsProvider rabbitMQWorkerSizeObserver = new RabbitMQUsageMetricsProvider(workerQueueName);
      workerSizeObserverProxy.addObserver(workerQueueName, rabbitMQWorkerSizeObserver);
      workerSizeObserverProxy.addObserver(workerQueueName, taskManagerMetricsRegister);
      workerSizeObserverProxy.addObserver(workerQueueName, workerPool);
      workerSizeObserverProxy.addObserver(workerQueueName, watchDog);
      // startup Guard should be the last observer added as it will unlock the task dispatcher
      workerSizeObserverProxy.addObserver(workerQueueName, startupGuard);

      if (taskScheduler instanceof final WorkerSizeObserver wzo) {
        workerSizeObserverProxy.addObserver(workerQueueName, wzo);
      }
      workerProducer.start();
      // Set up metrics
      WorkerPoolMetrics.setupMetrics(workerPool, workerQueueName);

      dispatcher = new TaskDispatcher(workerQueueName, taskScheduler, workerPool);
      executorService.execute(() -> {
        try {
          // Wait for worker queue to be empty.
          startupGuard.waitForOpen();
          LOG.info("Starting task scheduler {}: {}", taskScheduler.getClass().getSimpleName(), queueConfig);
          taskManagerMetrics.addRabbitMQUsageMetricsProvider(rabbitMQWorkerSizeObserver);
          taskManagerMetrics.addWorkerPoolUsageMetricsProvider(workerPool);
          taskManagerMetrics.addTaskManagerUsageMetricsProvider(taskManagerUsageMetrics);
          dispatcher.run();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }});
    }

    /**
     * @return true if the dispatcher is running.
     */
    public boolean isRunning() {
      return dispatcher.isRunning();
    }

    private void updateQueues(final List<T> newTaskQueues, final QueueConfig workerQueueConfig) {
      final Map<String, ? extends TaskQueue> newTaskQueuesMap = newTaskQueues.stream().filter(Objects::nonNull)
          .collect(Collectors.toMap(TaskQueue::getQueueName, Function.identity()));
      // Remove queues that are not in the new list
      final List<Entry<String, TaskConsumer>> removedQueues = taskConsumers.entrySet().stream().filter(e -> !newTaskQueuesMap.containsKey(e.getKey()))
          .toList();
      removedQueues.forEach(e -> removeTaskConsumer(e.getKey()));
      // Add and Update existing queues
      newTaskQueues.stream().filter(Objects::nonNull).forEach(tc -> addOrUpdateTaskQueue(tc, workerQueueConfig));
    }

    private void addOrUpdateTaskQueue(final T taskQueueConfiguration, final QueueConfig workerQueueConfig) {
      addTaskConsumerIfAbsent(new QueueConfig(taskQueueConfiguration.getQueueName(), workerQueueConfig.durable(), workerQueueConfig.eagerFetch(),
          workerQueueConfig.maxWorkersAvailable(), workerQueueConfig.queueType()));
      taskScheduler.updateQueue(taskQueueConfiguration);
    }

    /**
     * Adds a task consumer.
     *
     * @param queueConfig Configuration parameters for the queue
     */
    public void addTaskConsumerIfAbsent(final QueueConfig queueConfig) {
      taskConsumers.computeIfAbsent(queueConfig.queueName(), tqn -> {
        try {
          final TaskConsumer taskConsumer = new TaskConsumerImpl(executorService, queueConfig, dispatcher, factory);
          taskConsumer.start();
          LOG.info("Started task queue: {}", queueConfig);
          return taskConsumer;
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      });
    }


    /**
     * Removes a task consumer with the given queue name.
     *
     * @param workerQueueName queue name of the task consumer
     */
    private void removeTaskConsumer(final String taskQueueName) {
      LOG.info("Removed task queue {}", taskQueueName);
      taskScheduler.removeQueue(taskQueueName);
      taskConsumers.remove(taskQueueName).shutdown();
    }

    public void shutdown() {
      dispatcher.shutdown();
      workerProducer.shutdown();
      taskManagerMetrics.remove(workerQueueName);
      WorkerPoolMetrics.removeMetrics(workerQueueName);
      taskConsumers.forEach((k, v) -> v.shutdown());
    }

    /**
     * Test method to check if there is a task consumer for the given queue name.
     *
     * @param queueName name of the queue to check
     * @return true if the queue is present
     */
    boolean hasTaskConsumer(final String queueName) {
      return taskConsumers.containsKey(queueName);
    }
  }
}
