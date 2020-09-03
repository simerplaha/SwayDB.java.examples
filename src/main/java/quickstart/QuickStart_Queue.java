package quickstart;

import swaydb.java.Queue;
import swaydb.java.memory.MemoryQueue;

import java.time.Duration;

import static swaydb.java.serializers.Default.intSerializer;

public class QuickStart_Queue {

  public static void main(String[] args) {
    Queue<Integer> queue =
      MemoryQueue
        .config(intSerializer())
        .get();

    queue.push(1);
    queue.push(2, Duration.ofSeconds(0));
    queue.push(3);

    queue.pop(); //returns Optional(1)
    queue.pop(); //returns Optional(3) because 2 is expired.
  }
}
