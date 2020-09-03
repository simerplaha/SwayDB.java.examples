package quickstart;

import swaydb.KeyVal;
import swaydb.java.Map;
import swaydb.java.Stream;
import swaydb.java.memory.MemoryMap;

import java.time.Duration;

import static swaydb.java.serializers.Default.intSerializer;

class QuickStart_Map_Simple {

  public static void main(String[] args) {

    Map<Integer, Integer, Void> map =
      MemoryMap
        .functionsOff(intSerializer(), intSerializer())
        .get();

    map.put(1, 1); //basic put
    map.get(1).get(); //basic get
    map.expire(1, Duration.ofSeconds(1)); //basic expire
    map.remove(1); //basic remove

    //atomic write a Stream of key-value
    map.put(Stream.range(1, 100).map(KeyVal::create));

    //Create a stream that updates all values within range 10 to 90.
    Stream<KeyVal<Integer, Integer>> updatedKeyValues =
      map
        .stream()
        .from(10)
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
