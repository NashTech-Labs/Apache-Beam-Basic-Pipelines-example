## README.md

> Problem 1 -
Given CSV file of car ad data(find attached car_ad.csv file)

### Write a Beam pipeline to read the given CSV file of car ad data and compute the average price of different Cars. Save the output in another CSV file with headers [car avg_price]

### Output sample file-
car,avg_price Infiniti,29900.0 Ford,3500.0 Tesla,176900.0 TATA,112900.0

> Problem 5 -
Given CSV file of google stock 2020 data(find attached google_stock_20202.csv file)

### Write a Beam pipeline to read the given google_stock_20202.csv file and compute the average closing price of every month of year 2020. Save the output in another file with Header [month,avg_price] 
### Output sample file-

* month,avg_price

* 03,1032.42 04,1134.56

Hint: split the Date by "-" extract month(2nd index of String array)