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

public record QueueConfig(String queueName, boolean durable, boolean eagerFetch, int maxWorkersAvailable, RabbitMQQueueType queueType) {
  @Override
  public final String toString() {
    return String.format("Queue name:%s, durable:%b, eagerFetch:%b, maxWorkersAvailable:%d, queueType:%s", queueName, durable, eagerFetch,
        maxWorkersAvailable, queueType == null ? "default" : queueType.type());
  }
}
