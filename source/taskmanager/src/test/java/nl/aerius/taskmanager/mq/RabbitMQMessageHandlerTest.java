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
package nl.aerius.taskmanager.mq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

import nl.aerius.taskmanager.adaptor.TaskMessageHandler;
import nl.aerius.taskmanager.domain.Message;
import nl.aerius.taskmanager.domain.MessageReceivedHandler;
import nl.aerius.taskmanager.domain.QueueConfig;

/**
 * Test class for {@link RabbitMQMessageHandler}.
 */
@ExtendWith(MockitoExtension.class)
class RabbitMQMessageHandlerTest extends AbstractRabbitMQTest {
  final String taskQueueName = "queue1";

  private @Captor ArgumentCaptor<ShutdownListener> shutdownListenerCaptor;

  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  void testMessageReceivedHandler() throws IOException, InterruptedException {
    final byte[] receivedBody = "4321".getBytes();
    final TaskMessageHandler<?> tmh = adapterFactory.createTaskMessageHandler(new QueueConfig(taskQueueName, false, false, -1, null));
    final Semaphore lock = new Semaphore(0);
    final DataDock data = new DataDock();
    tmh.start();
    tmh.addMessageReceivedHandler(new MessageReceivedHandler() {
      @Override
      public void onMessageReceived(final Message message) {
        if (message instanceof RabbitMQMessage) {
          data.setData(((RabbitMQMessage) message).getBody());
        }
        lock.release(1);
      }

      @Override
      public void handleShutdownSignal() {
        // No-op
      }
    });
    mockChannel.basicPublish("", taskQueueName, new BasicProperties(), receivedBody);
    lock.tryAcquire(1, 5, TimeUnit.SECONDS);
    assertArrayEquals(receivedBody, data.getData(), "Test if body received");
  }

  /**
   * Unit test to test startup process with failing startup.
   * Flow:
   * <ol>
   * <li>Start and throw an IOException in basicConsume which is called on start.
   * <li>Should retry in start and end start without exceptions.
   * <li>Run start again, it should run without exceptions.
   * <li>Trigger a ShutdownSignalException, messageReceivedHandler#handleShutdownSignal. This method shouldn't have been called before.
   * </ol>
   */
  @Test
  @Timeout(1000)
  void testReStart() throws IOException, InterruptedException {
    // Lock used in the tryStartConsuming method to wait for test to allow to continue running
    final Semaphore tryStartConsumingLock = new Semaphore(0);
    // Lock to let the test wait till the TaskMessageHandler thread runs has started the consumer
    final Semaphore verifyTryStartConsumingLock = new Semaphore(0);
    // Counter to keep track how many times basicConsume has been called.
    final AtomicInteger throwCounter = new AtomicInteger();
    // Counter to keep track how many times shutdownCompleted method has been called.
    final AtomicInteger shutdownCallsCounter = new AtomicInteger();

    final MessageReceivedHandler mockMessageReceivedHandler = mock(MessageReceivedHandler.class);
    final TaskMessageHandler<?> tmh = adapterFactory.createTaskMessageHandler(new QueueConfig(taskQueueName, false, false, -1, null));

    ((RabbitMQMessageHandler) tmh).setRetryTimeMilliseconds(1L);
    doAnswer(invoke -> null).when(mockChannel).addShutdownListener(shutdownListenerCaptor.capture());
    doAnswer(invoke -> {
      verifyTryStartConsumingLock.release();
      tryStartConsumingLock.acquire();
      // stop consumer one time with an exception.
      if (throwCounter.incrementAndGet() < 2) {
        shutdownCallsCounter.incrementAndGet();
        final ShutdownSignalException exception = new ShutdownSignalException(false, false, null, null);

        // This will mock the shutdown handler is called.
        shutdownListenerCaptor.getValue().shutdownCompleted(exception);
        // This will mock starting the consumer failed with an exception.
        throw exception;
      }
      return null;
    }).when(mockChannel).basicConsume(anyString(), eq(false), anyString(), any());
    tmh.addMessageReceivedHandler(mockMessageReceivedHandler);
    final Thread thread = new Thread(() -> {
      try {
        // First start. Should retry once and then complete without error.
        tmh.start();
        // Second start. Should complete without error.
        tmh.start();
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    });

    thread.setDaemon(true);
    thread.start();
    // Wait till TaskMessageHandler has called basicConsume in the consumer.
    verifyTryStartConsumingLock.acquire();
    // Release the consumer start lock, it should throw an IOException and not call the shutdown handler.
    triggerRestartConsumer(tryStartConsumingLock, verifyTryStartConsumingLock, mockMessageReceivedHandler);
    // Release the second time, it should not throw an IOException this time, but just finish start without issue.
    triggerRestartConsumer(tryStartConsumingLock, verifyTryStartConsumingLock, mockMessageReceivedHandler);
    // Release the second start call. It should just finished normally.
    tryStartConsumingLock.release();
    // Wait for thread to finish.
    thread.join();

    // Trigger an ShutdownSignalException.
    shutdownListenerCaptor.getValue().shutdownCompleted(new ShutdownSignalException(false, false, null, null));

    verify(mockMessageReceivedHandler, only()).handleShutdownSignal();
    assertEquals(1, shutdownCallsCounter.get(), "Consumer basicConsume should have thrown the expected number of IOExceptions");
    assertEquals(3, throwCounter.get(), "Consumer basicConsume should have been called this of 3 times");

  }

  private static void triggerRestartConsumer(final Semaphore tryStartConsumingLock, final Semaphore verifyTryStartConsumingLock,
      final MessageReceivedHandler mockMessageReceivedHandler) throws InterruptedException {
    // Let the consumer basicConsume continue.
    tryStartConsumingLock.release();
    // Consumer should have restarted.
    verifyTryStartConsumingLock.acquire();
    // handleShutdownSignal should not have been called.
    verify(mockMessageReceivedHandler, never()).handleShutdownSignal();
  }
}
