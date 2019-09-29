# SwayDB.java 

[![Maven Central](https://img.shields.io/maven-central/v/com.github.javadev/swaydb-java.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.github.javadev%22%20AND%20a%3A%22swaydb-java%22)
[![Build Status](https://travis-ci.com/simerplaha/SwayDB.java.svg?branch=master)](https://travis-ci.com/simerplaha/SwayDB.java)
[![codecov.io](http://codecov.io/github/simerplaha/SwayDB.java/coverage.svg?branch=master)](http://codecov.io/github/simerplaha/SwayDB.java?branch=master)
[![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=SwayDB.java&metric=sqale_rating)](https://sonarcloud.io/dashboard/index/SwayDB.java)
[![Scrutinizer](https://img.shields.io/scrutinizer/g/simerplaha/SwayDB.java.svg)](https://scrutinizer-ci.com/g/simerplaha/SwayDB.java/)

[![Join the chat at https://gitter.im/SwayDB-chat/Lobby](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/SwayDB-chat/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Join the chat at https://swaydb.slack.com](https://img.shields.io/badge/slack-join%20chat-e01563.svg)](https://join.slack.com/t/swaydb/shared_invite/enQtNjM5MDM2MjYyMTE2LWU3ZTczNjA4YTAxZGNhMzk2MDc1MDViZTE0MzkyMmI2Y2E0OGE1ODg0MGJiZjY3YzY3MTE2MTA4MDcxZmMzMzY)

Java wrapper for [SwayDB](https://github.com/simerplaha/SwayDB).

Requirements
============

Java 1.8 and later.

## Installation

Include the following in your `pom.xml` for Maven:

```
<dependencies>
  <dependency>
    <groupId>com.github.javadev</groupId>
    <artifactId>swaydb-java</artifactId>
    <version>0.9-alpha.2</version>
  </dependency>
  ...
</dependencies>
```

Gradle:

```groovy
compile 'com.github.javadev:swaydb-java:0.9-alpha.2'
```

### Usage

```java
import java.util.AbstractMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class QuickStartTest {

    @Test
    public void quickStart() {
        // Create a memory database
        swaydb.java.Map<Integer, String> map = swaydb.java.memory.Map.create(Integer.class, String.class);

        map.put(1, "one");
        map.get(1);
        map.remove(1);

        // write 100 key-values atomically
        map.put(IntStream.rangeClosed(1, 100)
            .mapToObj(index -> new AbstractMap.SimpleEntry<>(index, String.valueOf(index)))
            .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue())));

        // Iteration: fetch all key-values withing range 10 to 90,
        // update values and atomically write updated key-values
        map
            .from(10)
            .takeWhile(item -> item.getKey() <= 90)
            .map(item -> {item.setValue(item.getValue() + "_updated"); return item;})
            .materialize()
            .foreach(map::put);

        // assert the key-values were updated
        IntStream.rangeClosed(10, 90)
                .mapToObj(item -> new AbstractMap.SimpleEntry<>(item, map.get(item)))
                .forEach(pair -> assertThat(pair.getValue().endsWith("_updated"), equalTo(true)));
    }
}
```
