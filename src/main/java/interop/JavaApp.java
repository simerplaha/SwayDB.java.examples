package interop;

import swaydb.Apply;
import swaydb.KeyVal;
import swaydb.PureFunction;
import swaydb.java.Map;
import swaydb.java.Stream;
import swaydb.java.memory.MemoryMap;

import java.util.Collections;

import static swaydb.java.serializers.Default.intSerializer;
import static swaydb.PureFunctionJava.*;

/**
 * Demos how Java SwayDB instances can be accessed in Scala.
 * <p>
 * This class simply creates a Java map with 100 key-values.
 * <p>
 * See ScalaApp.scala (commented out) to how to access this map within Scala.
 */
public class JavaApp {

  OnValue<Integer, Integer> incrementLikesFunction =
    (Integer currentLikes) ->
      Apply.update(currentLikes + 1);

  Map<Integer, Integer, PureFunction<Integer, Integer, Apply.Map<Integer>>> map;

  public JavaApp() {
    map =
      MemoryMap
        .functionsOn(intSerializer(), intSerializer(), Collections.singleton(incrementLikesFunction))
        .get();

    map.put(Stream.range(1, 100).map(KeyVal::of));
  }

  void applyFunctionInJava() {
    map.applyFunction(100, incrementLikesFunction);

    map
      .stream()
      .forEach(System.out::println);
  }
}
