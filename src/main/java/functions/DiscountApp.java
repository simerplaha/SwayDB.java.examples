package functions;


import swaydb.java.Deadline;
import swaydb.java.Map;
import swaydb.java.PureFunction;
import swaydb.java.Return;
import swaydb.java.memory.MapConfig;

import java.time.Duration;
import java.util.Optional;

import static swaydb.java.serializers.Default.doubleSerializer;
import static swaydb.java.serializers.Default.stringSerializer;

class DiscountApp {

  public static void main(String[] args) {
    //Our function that implements the update logic.
    //You can also use PureFunction.OnValue or PureFunction.OnKey.
    PureFunction.OnKeyValue<String, Double, Return.Map<Double>> discount =
      (String key, Double price, Optional<Deadline> deadline) -> {
        //if there are less than 2 days to expiry then apply discount.
        if (deadline.isPresent() && deadline.get().timeLeft().minusDays(2).isNegative()) {
          double discountedPrice = price * 0.50; //50% discount.
          if (discountedPrice <= 10) { //If the price is below $10
            //return Return.expire(Duration.ZERO); //expire it
            return Return.remove(); //or remove the product
          } else {
            return Return.update(discountedPrice); //else update with the discounted price
          }
        } else {
          return Return.nothing(); //else do nothing.
        }
      };

    //create our map with functions enabled.
    Map<String, Double, PureFunction<String, Double, Return.Map<Double>>> products =
      MapConfig.functionsOn(stringSerializer(), doubleSerializer())
        .registerFunction(discount) //register the discount function
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
