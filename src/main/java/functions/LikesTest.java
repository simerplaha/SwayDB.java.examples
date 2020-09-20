package functions;


import org.junit.jupiter.api.Test;
import swaydb.Apply;
import swaydb.PureFunction;
import swaydb.PureFunctionJava;
import swaydb.PureFunctionJava.OnValue;
import swaydb.java.Map;
import swaydb.java.memory.MemoryMap;

import java.util.Collections;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static swaydb.java.serializers.Default.intSerializer;
import static swaydb.java.serializers.Default.stringSerializer;

class LikesTest {

  @Test
  void likesCountTest() {
    //function that increments likes by 1
    //in SQL this would be "UPDATE LIKES_TABLE SET LIKES = LIKES + 1"
    OnValue<String, Integer> incrementLikesFunction =
      (Integer currentLikes) ->
        Apply.update(currentLikes + 1);

    Map<String, Integer, PureFunction<String, Integer, Apply.Map<Integer>>> likesMap =
      MemoryMap
        .functionsOn(stringSerializer(), intSerializer(), Collections.singleton(incrementLikesFunction))
        .get();

    likesMap.put("SwayDB", 0); //initial entry with 0 likes.

    //this could also be applied concurrently and the end result is the same.
    //applyFunction is atomic and thread-safe.
    IntStream
      .rangeClosed(1, 100)
      .parallel()
      .forEach(
        integer ->
          likesMap.applyFunction("SwayDB", incrementLikesFunction)
      );

    //assert the number of likes applied.
    assertEquals(100, likesMap.get("SwayDB").get());
  }
}
