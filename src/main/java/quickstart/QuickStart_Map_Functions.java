package quickstart;

import swaydb.Apply;
import swaydb.KeyVal;
import swaydb.PureFunction;
import swaydb.PureFunctionJava;
import swaydb.PureFunctionJava.OnKeyValueExpiration;
import swaydb.java.Map;
import swaydb.java.Stream;
import swaydb.java.memory.MemoryMap;

import java.time.Duration;
import java.util.Collections;

import static swaydb.java.serializers.Default.intSerializer;

class QuickStart_Map_Functions {

  public static void main(String[] args) {

    //create a function that reads key & value and applies modifications
    OnKeyValueExpiration<Integer, Integer> function =
      (key, value, expiration) -> {
        if (key < 25) { //remove if key is less than 25
          return Apply.removeFromMap();
        } else if (key < 50) { //expire after 2 seconds if key is less than 50
          return Apply.expireFromMap(Duration.ofSeconds(2));
        } else if (key < 75) { //update if key is < 75.
          return Apply.update(value + 10000000);
        } else { //else do nothing
          return Apply.nothingOnMap();
        }
      };

    Map<Integer, Integer, PureFunction<Integer, Integer, Apply.Map<Integer>>> map =
      MemoryMap
        .functionsOn(intSerializer(), intSerializer(), Collections.singleton(function))
        .get();

    map.put(1, 1); //basic put
    map.get(1).get(); //basic get
    map.expire(1, Duration.ofSeconds(1)); //basic expire
    map.remove(1); //basic remove

    //atomic write a Stream of key-value
    map.put(Stream.range(1, 100).map(KeyVal::of));

    //Create a stream that updates all values within range 10 to 90.
    Stream<KeyVal<Integer, Integer>> updatedKeyValues =
      map
        .stream()
        .from(10)
        .takeWhile(keyVal -> keyVal.key() <= 90)
        .map(keyVal -> KeyVal.of(keyVal.key(), keyVal.value() + 5000000));

    //submit the stream to update the key-values as a single transaction.
    map.put(updatedKeyValues);

    map.applyFunction(1, 100, function); //apply the function to all key-values ranging 1 to 100.

    //print all key-values to view the update.
    map
      .stream()
      .forEach(System.out::println);
  }
}
