package quickstart;

import java.time.Duration;

import swaydb.java.*;
import swaydb.java.memory.MapConfig;
import static swaydb.java.serializers.Default.intSerializer;

class QuickStart_Map_Simple {

  public static void main(String[] args) {

    Map<Integer, Integer, Void> map =
      MapConfig
        .withoutFunctions(intSerializer(), intSerializer())
        .init();

    map.put(1, 1); //basic put
    map.get(1).get(); //basic get
    map.expire(1, Duration.ofSeconds(1)); //basic expire
    map.remove(1); //basic remove

    //atomic write a Stream of key-value
    map.put(Stream.range(1, 100).map(KeyVal::create));

    //Create a stream that updates all values within range 10 to 90.
    Stream<KeyVal<Integer, Integer>> updatedKeyValues =
      map
        .from(10)
        .stream()
        .takeWhile(keyVal -> keyVal.key() <= 90)
        .map(keyVal -> KeyVal.create(keyVal.key(), keyVal.value() + 5000000));

    //submit the stream to update the key-values as a single transaction.
    map.put(updatedKeyValues);

    //print all key-values to view the update.
    map
      .stream()
      .forEach(System.out::println);
  }
}
