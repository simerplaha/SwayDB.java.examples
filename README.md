# SwayDB.java.examples [![Slack Chat][slack-badge]][slack-link] [![Gitter Chat][gitter-badge]][gitter-link] [![Build status][build-badge]][build-link] [![Maven central][maven-badge]][maven-link]

[gitter-badge]: https://badges.gitter.im/Join%20Chat.svg
[gitter-link]: https://gitter.im/SwayDB-chat/Lobby

[slack-badge]: https://img.shields.io/badge/slack-join%20chat-e01563.svg
[slack-link]: https://join.slack.com/t/swaydb/shared_invite/enQtNzI1NzM1NTA0NzQxLTJiNjRhMDg2NGQ3YzBkNGMxZGRmODlkN2M3MWEwM2U2NWY1ZmU5OWEyYTgyN2ZhYjlhNjdlZTM3YWJjMGZmNzQ

[maven-badge]: https://img.shields.io/maven-central/v/io.swaydb/java_2.12.svg
[maven-link]: https://search.maven.org/search?q=g:io.swaydb%20AND%20a:java_2.12

[build-badge]: https://github.com/simerplaha/SwayDB.java.examples/workflows/Java%20CI/badge.svg
[build-link]: https://github.com/simerplaha/SwayDB.java.examples/actions?query=workflow%3A%22Java+CI%22

Implements examples demoing [SwayDB](https://github.com/simerplaha/SwayDB)'s Java API.

Requirements
============

Java 1.8 and later.

### Quick start example.

See [QuickStart.java](/src/main/java/quickstart/QuickStart_Map_Simple.java).

```java
Map<Integer, Integer, Void> map =
  MapConfig
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
```
