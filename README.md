# SwayDB.java.examples [![Slack Chat][slack-badge]][slack-link] [![Gitter Chat][gitter-badge]][gitter-link] [![Build status][build-badge]][build-link] [![Maven central][maven-badge]][maven-link]

[gitter-badge]: https://badges.gitter.im/Join%20Chat.svg
[gitter-link]: https://gitter.im/SwayDB-chat/Lobby

[slack-badge]: https://img.shields.io/badge/slack-join%20chat-e01563.svg
[slack-link]: https://join.slack.com/t/swaydb/shared_invite/enQtNzI1NzM1NTA0NzQxLTJiNjRhMDg2NGQ3YzBkNGMxZGRmODlkN2M3MWEwM2U2NWY1ZmU5OWEyYTgyN2ZhYjlhNjdlZTM3YWJjMGZmNzQ

[maven-badge]: https://img.shields.io/maven-central/v/io.swaydb/java_2.12.svg
[maven-link]: https://search.maven.org/search?q=g:io.swaydb%20AND%20a:java_2.12

[build-badge]: https://travis-ci.com/simerplaha/SwayDB.java.examples.svg?branch=master
[build-link]: https://travis-ci.com/simerplaha/SwayDB.java.examples

Implements examples demoing [SwayDB](https://github.com/simerplaha/SwayDB)'s Java API.

Requirements
============

Java 1.8 and later.

### Quick start example.

See [QuickStart.java](/src/main/java/quickstart/QuickStart.java).

```java
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

```
