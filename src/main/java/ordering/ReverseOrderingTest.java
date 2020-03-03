package ordering;


import org.junit.jupiter.api.Test;
import swaydb.java.Map;
import swaydb.java.memory.MapConfig;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static swaydb.java.serializers.Default.intSerializer;

class ReverseOrderingTest {

  @Test
  void reverse() {
    Map<Integer, Integer, Void> map =
      MapConfig.functionsOff(intSerializer(), intSerializer())
        //provide a typed comparator that reverses ordering
        .setTypedComparator((Integer key1, Integer key2) -> key1.compareTo(key2) * -1)
        .get();

    //insert in natural ordering from 1 to 100
    IntStream
      .rangeClosed(1, 100)
      .forEach(integer -> map.put(integer, integer));

    List<Integer> actual =
      map
        .keys()
        .stream()
        .materialize();

    //print out the stream. Since ordering is in reverse this will print from 100 to 1.
    actual.forEach(System.out::println);

    List<Integer> expected =
      IntStream
        .rangeClosed(1, 100)
        .map(i -> 100 - i + 1)
        .boxed()
        .collect(Collectors.toList());

    assertEquals(expected, actual);
  }
}
