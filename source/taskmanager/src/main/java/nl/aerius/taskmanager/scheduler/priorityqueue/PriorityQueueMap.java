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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import nl.aerius.taskmanager.domain.PriorityTaskQueue;
import nl.aerius.taskmanager.domain.TaskRecord;

/**
 * Map to keep track of queue configurations and number of tasks running on workers.
 */
class PriorityQueueMap<K extends PriorityQueueMapKeyMapper> {
  /**
   * Map to keep track of queue configuration per queue.
   */
  private final Map<String, PriorityTaskQueue> taskQueueConfigurations = new ConcurrentHashMap<>();
  /**
   * Map to keep track of the number of tasks running on workers per queue.
   */
  private final Map<String, AtomicInteger> tasksOnWorkersPerQueue = new ConcurrentHashMap<>();
  private final K keyMapper;

  public PriorityQueueMap() {
    this((K) new PriorityQueueMapKeyMapper());
  }

  public PriorityQueueMap(final K keyMapper) {
    this.keyMapper = keyMapper;
  }

  public PriorityTaskQueue get(final TaskRecord taskRecord) {
    return taskQueueConfigurations.get(taskRecord.queueName());
  }

  public boolean containsKey(final String queueName) {
    return taskQueueConfigurations.containsKey(queueName);
  }

  public PriorityTaskQueue put(final String queueName, final PriorityTaskQueue queue) {
    return taskQueueConfigurations.put(queueName, queue);
  }

  public void decrementOnWorker(final TaskRecord taskRecord) {
    final String trKey = key(taskRecord);

    if (Optional.ofNullable(tasksOnWorkersPerQueue.get(trKey)).map(v -> v.decrementAndGet() == 0).orElse(false)) {
      tasksOnWorkersPerQueue.remove(trKey);
    }
  }

  public void incrementOnWorker(final TaskRecord taskRecord) {
    tasksOnWorkersPerQueue.computeIfAbsent(key(taskRecord), k -> new AtomicInteger()).incrementAndGet();
  }

  /**
   * Returns the number of tasks on the worker as known by the taskmanager.
   *
   * @return total number of tasks on the worker
   */
  public int onWorkerTotal() {
    return tasksOnWorkersPerQueue.entrySet().stream().mapToInt(e -> e.getValue().get()).sum();
  }

  public int onWorkerByQueue(final String queueName) {
    return tasksOnWorkersPerQueue.entrySet().stream()
        .filter(e -> keyMapper.queueName(e.getKey()).equals(queueName))
        .mapToInt(e -> e.getValue().get())
        .sum();
  }

  public void reset() {
    tasksOnWorkersPerQueue.forEach((k, v) -> v.set(0));
  }

  public int onWorker(final TaskRecord taskRecord) {
    return Optional.ofNullable(tasksOnWorkersPerQueue.get(key(taskRecord))).map(AtomicInteger::intValue).orElse(0);
  }

  public void remove(final String queueName) {
    taskQueueConfigurations.remove(queueName);
  }

  private String key(final TaskRecord taskRecord) {
    return keyMapper.key(taskRecord);
  }
}
