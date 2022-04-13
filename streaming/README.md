<h2> Streaming </h2> <br>

<p> 1. docker-compose up --build </p> <br>
<p> 2. docker exec -it spark-master bash </p> <br>
<p> 3. /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /home/streaming/file_name.py </p> <br>

<p> Questions that streaming processing answers: </p>
<p> 1. The average temperature in the month in which it was shown that there are the most accidents in New York to check if it is too cold </p>
<p> 2. How many days were the bad weather in the month with the highest number of collisions (sub-zero temperatures and snow) </p>
<p> 3. Number of days in the month with the most collisions when the daily rainfall is above 2.5mm </p>
<p> 4. Average visibility rating on the road in the month with the most collisions </p>
