package quickstart;

import swaydb.java.MapIO;
import swaydb.java.PureFunction;
import swaydb.java.Stream;
import swaydb.java.data.util.KeyVal;

import java.time.Duration;

import static swaydb.java.serializers.Default.intSerializer;

class QuickStart {

  public static void main(String[] args) {
    //create a memory database.
    MapIO<Integer, Integer, PureFunction.VoidM<Integer, Integer>> map =
      swaydb.java.memory.Map
        .config(intSerializer(), intSerializer())
        .init()
        .get();

    //basic put and expire
    map.put(1, 1, Duration.ofSeconds(1)).get();
    map.get(1).get(); //get
    map.remove(1).get(); //remove

    //atomic write a Stream of key-value
    map.put(Stream.range(1, 100).map(KeyVal::create)).get();

    //create a read stream from 10th key-value to 90th, increment values by 1000000 and insert.
    map
      .from(10)
      .takeWhile(keyVal -> keyVal.key() <= 90)
      .map(keyVal -> KeyVal.create(keyVal.key(), keyVal.value() + 1000000))
      .materialize()
      .flatMap(map::put)
      .get();

    //print all key-values
    map
      .forEach(System.out::println)
      .materialize()
      .get();

    //stop app.
    System.exit(0);
  }
}
