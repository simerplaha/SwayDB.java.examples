package functions;


import swaydb.Apply;
import swaydb.Expiration;
import swaydb.PureFunction;
import swaydb.PureFunctionJava.OnKeyValueExpiration;
import swaydb.java.Map;
import swaydb.java.memory.MemoryMap;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import static swaydb.java.serializers.Default.doubleSerializer;
import static swaydb.java.serializers.Default.stringSerializer;

class DiscountApp {

  public static void main(String[] args) {
    //Our function that implements the update logic.
    //You can also use PureFunction.OnValue or PureFunction.OnKey.
    OnKeyValueExpiration<String, Double> discount =
      (String key, Double price, Optional<Expiration> expiration) -> {
        //if there are less than 2 days to expiry then apply discount.
        if (expiration.isPresent() && expiration.get().timeLeft().minusDays(2).isNegative()) {
          double discountedPrice = price * 0.50; //50% discount.
          if (discountedPrice <= 10) { //If the price is below $10
            //return Apply.expire(Duration.ZERO); //expire it
            return Apply.removeFromMap(); //or remove the product
          } else {
            return Apply.update(discountedPrice); //else update with the discounted price
          }
        } else {
          return Apply.nothingOnMap(); //else do nothing.
        }
      };

    //create our map with functions enabled.
    Map<String, Double, PureFunction<String, Double, Apply.Map<Double>>> products =
      MemoryMap
        .functionsOn(stringSerializer(), doubleSerializer(), Collections.singleton(discount))
        .get();

    //insert two products that expire after a day.
    products.put("MacBook Pro", 2799.00, Duration.ofDays(1));
    products.put("Tesla", 69275.0, Duration.ofDays(1));

    //apply the discount function.
    products.applyFunction("MacBook Pro", "Tesla", discount);

    //print em
    products.stream().forEach(System.out::println);
  }

}
