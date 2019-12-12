//package interop
//
///**
// * Test app that demos how a database created in Java can interop with Scala.
// */
//object ScalaApp extends App {
//
//  //create an instance of JavaApp.
//  val javaApp = new JavaApp()
//  //scala map from java
//  val scalaMap = javaApp.map.asScala
//
//  //access the map within the JavaApp call and invoke asScala to get the Scala instance of the Map.
//  scalaMap
//    .foreach(println)
//    .materialize
//    .get
//
//  import swaydb.java.Interop._
//  //register java function in Scala.
//  scalaMap.registerFunction(javaApp.incrementLikesFunction.asScala).get
//  //apply the above registered function in Java.
//  javaApp.applyFunctionInJava()
//
//}
