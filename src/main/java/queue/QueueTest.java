package queue;

import org.junit.jupiter.api.Test;
import swaydb.java.Queue;
import swaydb.java.memory.QueueConfig;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static swaydb.java.serializers.Default.intSerializer;

class QueueTest {

  @Test
  void quickStart() {
    Queue<Integer> queue =
      QueueConfig.configure(intSerializer())
        .init();

    queue.push(1);
    queue.push(2, Duration.ofSeconds(0));
    queue.push(3);

    assertEquals(2, queue.popOrNull()); //2
    assertEquals(3, queue.popOrNull()); //returns 3 because 2 is expired.
  }
}
