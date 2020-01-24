package functions;


import org.junit.jupiter.api.Test;
import swaydb.java.Map;
import swaydb.java.PureFunction;
import swaydb.java.Return;
import swaydb.java.memory.MapConfig;

import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static swaydb.java.serializers.Default.intSerializer;
import static swaydb.java.serializers.Default.stringSerializer;

class LikesTest {

  @Test
  void likesCountTest() {

    Map<String, Integer, PureFunction<String, Integer, Return.Map<Integer>>> likesMap =
      MapConfig.withFunctions(stringSerializer(), intSerializer())
        .init();

    likesMap.put("SwayDB", 0); //initial entry with 0 likes.

    //function that increments likes by 1
    //in SQL this would be "UPDATE LIKES_TABLE SET LIKES = LIKES + 1"
    PureFunction.OnValue<String, Integer, Return.Map<Integer>> incrementLikesFunction =
      currentLikes ->
        Return.update(currentLikes + 1);

    //register the above likes function
    likesMap.registerFunction(incrementLikesFunction);

    //this could also be applied concurrently and the end result is the same.
    //applyFunction is atomic and thread-safe.
    IntStream
      .rangeClosed(1, 100)
      .forEach(
        integer ->
          likesMap.applyFunction("SwayDB", incrementLikesFunction)
      );

    //assert the number of likes applied.
    assertEquals(100, likesMap.get("SwayDB").get());
  }
}
