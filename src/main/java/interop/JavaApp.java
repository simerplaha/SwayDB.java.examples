package interop;

import swaydb.java.*;

import static swaydb.java.serializers.Default.intSerializer;

/**
 * Demos how Java SwayDB instances can be accessed in Scala.
 * <p>
 * This class simply creates a Java map with 100 key-values.
 * <p>
 * See ScalaApp.scala (commented out) to how to access this map within Scala.
 */
public class JavaApp {

  PureFunction.OnValue<Integer, Integer, Return.Map<Integer>> incrementLikesFunction =
    currentLikes ->
      Return.update(currentLikes + 1);

  Map<Integer, Integer, PureFunction<Integer, Integer, Return.Map<Integer>>> map;

  public JavaApp() {
    map =
      swaydb.java.memory.MapConfig
        .withFunctions(intSerializer(), intSerializer())
        .init();

    map.put(Stream.range(1, 100).map(KeyVal::create));
  }

  void applyFunctionInJava() {
    map.applyFunction(100, incrementLikesFunction);

    map
      .stream()
      .forEach(System.out::println);
  }
}
