package queue;

import org.junit.jupiter.api.Test;
import swaydb.java.Queue;
import swaydb.java.memory.QueueConfig;

import java.time.Duration;

import static swaydb.java.serializers.Default.intSerializer;

class QueueExample {

  @Test
  void quickStart() {
    Queue<Integer> queue =
      QueueConfig.configure(intSerializer())
        .init();

    queue.push(1);
    queue.push(2, Duration.ofSeconds(0));
    queue.push(3);

    queue.pop(); //returns Optional(2)
    queue.pop(); //returns Optional(3) because 2 is expired.
  }
}
