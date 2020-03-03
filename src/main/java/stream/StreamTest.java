package stream;

import org.junit.jupiter.api.Test;
import swaydb.java.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamTest {

  @Test
  void stream() {
    Integer sum =
      Stream
        .range(1, 1000)
        .map(integer -> integer - 1)
        .filter(integer -> integer % 2 == 0)
        .foldLeft(0, Integer::sum);

    assertEquals(249500, sum);
  }
}
