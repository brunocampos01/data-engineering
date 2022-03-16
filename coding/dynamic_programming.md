# Dynamic Programming

## Knapsack Problem

![image](https://user-images.githubusercontent.com/12896018/158519757-fabd7fda-68cf-4682-97ba-3c433dc83754.png)

You’re a thief with a knapsack that can carry 4 lb of goods.
<br/>
You have three items that you can put into the knapsack.

![image](https://user-images.githubusercontent.com/12896018/158519850-ed908f31-ca96-4405-bb45-87cd6f7efc5d.png)

What items should you steal so that you steal the maximum money’s worth of goods? 

## Simple Solution
The simplest algorithm is this: you try every possible set of goods and find the set that gives you the most value.
![image](https://user-images.githubusercontent.com/12896018/158520137-c3680ab1-06ef-49f7-b1e1-15d9030a385e.png)

This works, but it’s really slow. For 3 items, you have to calculate 8 possible sets. For 4 items, you have to calculate 16 sets. With every
item you add, the number of sets you have to calculate doubles! This algorithm takes O(2^n) time, which is very, very slow.


