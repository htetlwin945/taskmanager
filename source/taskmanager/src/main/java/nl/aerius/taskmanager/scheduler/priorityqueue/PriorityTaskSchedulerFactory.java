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
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.function.Function;

import nl.aerius.taskmanager.domain.PriorityTaskQueue;
import nl.aerius.taskmanager.domain.PriorityTaskSchedule;
import nl.aerius.taskmanager.domain.QueueConfig;
import nl.aerius.taskmanager.domain.Task;
import nl.aerius.taskmanager.scheduler.TaskScheduler;
import nl.aerius.taskmanager.scheduler.TaskScheduler.TaskSchedulerFactory;

/**
 * Factory to create a scheduler.
 */
public class PriorityTaskSchedulerFactory implements TaskSchedulerFactory<PriorityTaskQueue, PriorityTaskSchedule> {
  private static final int INITIAL_SIZE = 20;

  private final PriorityTaskSchedulerFileHandler handler = new PriorityTaskSchedulerFileHandler();

  @Override
  public TaskScheduler<PriorityTaskQueue> createScheduler(final QueueConfig queueConfig) {
    final Function<Comparator<Task>, Queue<Task>> queueCreator = c -> createQueue(queueConfig.eagerFetch(), c);
    final PriorityQueueMap<?> priorityQueueMap = createPriorityQueueMap(queueConfig.eagerFetch());

    return new PriorityTaskScheduler(priorityQueueMap, queueCreator, queueConfig.queueName(), queueConfig.maxWorkersAvailable());
  }

  private static Queue<Task> createQueue(final boolean eagerFetch, final Comparator<Task> c) {
    return  eagerFetch ? new GroupedPriorityQueue(INITIAL_SIZE, c) : new PriorityQueue<>(INITIAL_SIZE, c);
  }

  private static PriorityQueueMap<?> createPriorityQueueMap(final boolean eagerFetch) {
    return eagerFetch ? new PriorityQueueMap<>(new EagerFetchPriorityQueueMapKeyMapper()) : new PriorityQueueMap<>();
  }

  @Override
  public PriorityTaskSchedulerFileHandler getHandler() {
    return handler;
  }
}
