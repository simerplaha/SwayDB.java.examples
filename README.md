# SwayDB.java.examples 

[![Maven Central](https://img.shields.io/maven-central/v/com.github.javadev/swaydb-java.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.github.javadev%22%20AND%20a%3A%22swaydb-java%22)
[![Build Status](https://travis-ci.com/simerplaha/SwayDB.java.svg?branch=master)](https://travis-ci.com/simerplaha/SwayDB.java)
[![Join the chat at https://gitter.im/SwayDB-chat/Lobby](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/SwayDB-chat/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Join the chat at https://swaydb.slack.com](https://img.shields.io/badge/slack-join%20chat-e01563.svg)](https://join.slack.com/t/swaydb/shared_invite/enQtNjM5MDM2MjYyMTE2LWU3ZTczNjA4YTAxZGNhMzk2MDc1MDViZTE0MzkyMmI2Y2E0OGE1ODg0MGJiZjY3YzY3MTE2MTA4MDcxZmMzMzY)

Implements examples demoing [SwayDB](https://github.com/simerplaha/SwayDB)'s Java API.

Requirements
============

Java 1.8 and later.

### Quick start example.

See [QuickStart.java](/src/test/java/swaydb/quickstart/QuickStart.java).

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
