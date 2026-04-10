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
package nl.aerius.taskmanager.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Base class for a single schedule configuration.
 * @param <T> specific task queue configuration class
 */
public class TaskSchedule<T extends TaskQueue> {

  private String workerQueueName;

  private Boolean durable;

  private Boolean eagerFetch;

  /*
   * Maximum number of workers available when the system is fully scaled.
   */
  private int maxWorkersAvailable;

  private RabbitMQQueueType queueType;

  private List<T> queues = new ArrayList<>();

  public String getWorkerQueueName() {
    return workerQueueName;
  }

  public void setWorkerQueueName(final String workerQueueName) {
    this.workerQueueName = workerQueueName;
  }

  public List<T> getQueues() {
    return queues;
  }

  public void setQueues(final List<T> queues) {
    this.queues = queues;
  }

  public void setDurable(final boolean durable) {
    this.durable = durable;
  }

  public boolean isDurable() {
    return !Boolean.FALSE.equals(durable);
  }

  public void setEagerFetch(final Boolean eagerFetch) {
    this.eagerFetch = eagerFetch;
  }

  public boolean isEagerFetch() {
    return Boolean.TRUE.equals(eagerFetch);
  }

  public RabbitMQQueueType getQueueType() {
    return queueType;
  }

  public void setQueueType(final String queueType) {
    this.queueType = RabbitMQQueueType.valueOf(queueType.toUpperCase(Locale.ROOT));
  }

  public int getMaxWorkersAvailable() {
    return maxWorkersAvailable;
  }

  public void setMaxWorkersAvailable(final int maxWorkersAvailable) {
    this.maxWorkersAvailable = maxWorkersAvailable;
  }

  @Override
  public String toString() {
    return "workerQueueName=" + workerQueueName + ", durable=" + durable + ", eagerFetch=" + eagerFetch + ", maxWorkersAvailable="
        + maxWorkersAvailable + ", queueType=" + queueType;
  }

}
