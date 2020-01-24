package quickstart;

import swaydb.java.*;

import java.time.Duration;
import java.util.List;

import static swaydb.java.serializers.Default.intSerializer;

class QuickStart {

  public static void main(String[] args) {
    //create a memory database.
    Map<Integer, Integer, PureFunction<Integer, Integer, Return.Map<Integer>>> map =
      swaydb.java.memory.MapConfig
        .withFunctions(intSerializer(), intSerializer())
        .init();

    map.put(1, 1); //basic put
    map.get(1).get(); //basic get
    map.expire(1, Duration.ofSeconds(1)); //basic expire
    map.remove(1); //basic remove

    //atomic write a Stream of key-value
    map.put(Stream.range(1, 100).map(KeyVal::create));

    //create a read stream from 10th key-value to 90th, increment values by 1000000 and insert.
    List<KeyVal<Integer, Integer>> updatedKeyValues =
      map
        .from(10)
        .stream()
        .takeWhile(keyVal -> keyVal.key() <= 90)
        .map(keyVal -> KeyVal.create(keyVal.key(), keyVal.value() + 5000000))
        .materialize();

    //write updated key-values
    map.put(updatedKeyValues);

    //create a function that reads key & value and applies modifications
    PureFunction.OnKeyValue<Integer, Integer, Return.Map<Integer>> function =
      (key, value, deadline) -> {
        if (key < 25) { //remove if key is less than 25
          return Return.remove();
        } else if (key < 50) { //expire after 2 seconds if key is less than 50
          return Return.expire(Duration.ofSeconds(2));
        } else if (key < 75) { //update if key is < 75.
          return Return.update(value + 10000000);
        } else { //else do nothing
          return Return.nothing();
        }
      };

    map.registerFunction(function); //register the function.

    map.applyFunction(1, 100, function); //apply the function to all key-values ranging 1 to 100.

    //print all key-values to view the update.
    map
      .stream()
      .forEach(System.out::println)
      .materialize();
  }
}
