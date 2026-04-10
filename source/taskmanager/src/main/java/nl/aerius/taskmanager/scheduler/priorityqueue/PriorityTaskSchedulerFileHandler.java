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

import java.io.File;
import java.io.IOException;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import nl.aerius.taskmanager.domain.PriorityTaskQueue;
import nl.aerius.taskmanager.domain.PriorityTaskSchedule;
import nl.aerius.taskmanager.scheduler.SchedulerFileConfigurationHandler;

/**
 * Handler to read and write Priority Task Scheduler configuration files.
 */
public class PriorityTaskSchedulerFileHandler implements SchedulerFileConfigurationHandler<PriorityTaskQueue, PriorityTaskSchedule> {

  private static final Logger LOG = LoggerFactory.getLogger(PriorityTaskSchedulerFileHandler.class);

  private static final String ENV_PREFIX = "AERIUS_PRIORITY_TASK_SCHEDULER_";

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public PriorityTaskSchedule read(final File file) throws IOException {
    final PriorityTaskSchedule fileSchedule = readFromFile(file);
    final PriorityTaskSchedule environmentSchedule = readFromEnvironment(fileSchedule.getWorkerQueueName());
    return environmentSchedule == null ? fileSchedule : environmentSchedule;
  }

  private PriorityTaskSchedule readFromFile(final File file) throws IOException {
    return objectMapper.readValue(file, PriorityTaskSchedule.class);
  }

  private PriorityTaskSchedule readFromEnvironment(final String workerQueueName) throws JsonProcessingException {
    final String environmentKey = ENV_PREFIX + workerQueueName.toUpperCase(Locale.ROOT);
    final String environmentValue = System.getenv(environmentKey);

    if (environmentValue != null) {
      LOG.info("Using configuration for worker queue {} from environment", workerQueueName);
      return objectMapper.readValue(environmentValue, PriorityTaskSchedule.class);
    }
    return null;
  }
}
