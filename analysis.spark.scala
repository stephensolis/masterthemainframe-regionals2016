import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

val clients = sqlContext.read.orc("clients.orc")
val transactions = sqlContext.read.orc("transactions.orc")

clients.registerTempTable("clients")
transactions.registerTempTable("transactions")

val transactionsWithClients = transactions.join(clients, "client_id")
val clientsWithCards = clients.join(transactions.select($"client_id", $"card_brand").distinct, "client_id")


println


println("1. Transaction volume by day of month:")

transactionsWithClients.groupBy(dayofmonth($"time").as("day_of_month")).agg(format_number(sum($"amount"), 2).as("total_tx_amount")).show(100, false)

sqlContext.sql("""
	SELECT dayofmonth(time) AS day_of_month, 
		   format_number(SUM(amount), 2) AS total_tx_amount
	FROM transactions 
		 INNER JOIN clients 
		 ON transactions.client_id = clients.client_id
	GROUP BY dayofmonth(time)
	ORDER BY dayofmonth(time)
""").show(100, false)


println("2. Transaction volume by weekday for movie transactions:")

transactionsWithClients.filter("merch_name = 'Movies'").groupBy(date_format($"time", "EEEE").as("weekday")).agg(format_number(sum($"amount"), 2).as("total_tx_amount")).show(100, false)

sqlContext.sql("""
	SELECT date_format(time, 'EEEE') AS weekday, 
		   format_number(SUM(amount), 2) AS total_tx_amount
	FROM transactions 
		 INNER JOIN clients 
		 ON transactions.client_id = clients.client_id
	WHERE merch_name = 'Movies'
	GROUP BY date_format(time, 'EEEE')
	ORDER BY FIRST(date_format(time, 'u'))
""").show(100, false)


println("3. Transaction volume per month for some categories:")

transactionsWithClients.groupBy(month($"time").as("month")).agg(format_number(sum($"amount"), 2).as("overall")).join(
	transactionsWithClients.filter("merch_cat_id BETWEEN 3000 AND 3999").groupBy(month($"time").as("month")).agg(format_number(sum($"amount"), 2).as("travel")), Seq("month"), "outer").join(
	transactionsWithClients.filter("merch_name = 'Plumbing and Heating'").groupBy(month($"time").as("month")).agg(format_number(sum($"amount"), 2).as("heating_and_plumbing")), Seq("month"), "outer").join(
	transactionsWithClients.filter("merch_name IN ('House of Drapery', 'Home of Crystal', 'All for your House', 'My Beatiful House')").groupBy(month($"time").as("month")).agg(format_number(sum($"amount"), 2).as("home_furnishings")), Seq("month"), "outer").show(100, false)

sqlContext.sql("""
	SELECT *
	FROM (SELECT month(time) AS month, 
				 format_number(SUM(amount), 2) AS overall
		  FROM transactions 
			   INNER JOIN clients 
			   ON transactions.client_id = clients.client_id
		  GROUP BY month(time)) AS T1
	FULL OUTER JOIN (SELECT month(time) AS month, 
							format_number(SUM(amount), 2) AS travel
					 FROM transactions 
						  INNER JOIN clients 
						  ON transactions.client_id = clients.client_id
					 WHERE merch_cat_id BETWEEN 3000 AND 3999
					 GROUP BY month(time)) AS T2 ON T1.month = T2.month
	FULL OUTER JOIN (SELECT month(time) AS month, 
							format_number(SUM(amount), 2) AS heating_and_plumbing
					 FROM transactions 
						  INNER JOIN clients 
						  ON transactions.client_id = clients.client_id
					 WHERE merch_name = 'Plumbing and Heating'
					 GROUP BY month(time)) AS T3 ON T1.month = T3.month
	FULL OUTER JOIN (SELECT month(time) AS month, 
							format_number(SUM(amount), 2) AS home_furnishings
					 FROM transactions 
						  INNER JOIN clients 
						  ON transactions.client_id = clients.client_id
					 WHERE merch_name IN ('House of Drapery', 'Home of Crystal', 'All for your House', 'My Beatiful House')
					 GROUP BY month(time)) AS T4 ON T1.month = T4.month
	ORDER BY T1.month
""").show(100, false)


println("4. Statistics per card brand:")

transactionsWithClients.groupBy($"card_brand").agg(
	format_number(avg($"amount"), 2).as("avg_tx_amount"),
	format_number(sum($"amount") / countDistinct($"client_id"), 2).as("client_avg_tx_sum"), 
	format_number(count($"amount") / countDistinct($"client_id"), 1).as("client_avg_tx_count")
).join(clientsWithCards.groupBy($"card_brand").agg(format_number(avg($"income"), 2).as("client_avg_income")), "card_brand").show(100, false)

sqlContext.sql("""
	SELECT *
	FROM (SELECT card_brand, 
				 format_number(AVG(amount), 2) AS avg_tx_amount, 
				 format_number(SUM(amount) / COUNT(DISTINCT clients.client_id), 2) AS client_avg_tx_sum,
				 format_number(COUNT(amount) / COUNT(DISTINCT clients.client_id), 1) AS client_avg_tx_count
		  FROM transactions 
			   INNER JOIN clients 
			   ON transactions.client_id = clients.client_id
		  GROUP BY card_brand) AS T1
	INNER JOIN (SELECT card_brand, 
					   format_number(AVG(income), 2) AS client_avg_income
				FROM clients 
					 INNER JOIN (SELECT DISTINCT client_id, card_brand FROM transactions) AS client_cards 
					 ON clients.client_id = client_cards.client_id
				GROUP BY card_brand) AS T2 ON T1.card_brand = T2.card_brand
	ORDER BY T1.card_brand
""").show(100, false)


println("5. Average transaction volume by gender for some categories:")

transactionsWithClients.filter("merch_name IN ('Donna Affascinante', 'Donna Elegante')").groupBy($"gender").agg(format_number(sum($"amount") / countDistinct($"client_id"), 2).as("womens_clothing")).join(
	transactionsWithClients.filter("merch_name = 'Uomo Elegante'").groupBy($"gender").agg(format_number(sum($"amount") / countDistinct($"client_id"), 2).as("mens_clothing")), "gender").join(
	transactionsWithClients.filter("merch_name = 'Cosmetic Stores'").groupBy($"gender").agg(format_number(sum($"amount") / countDistinct($"client_id"), 2).as("cosmetics")), "gender").join(
	transactionsWithClients.filter("merch_name IN ('Hardware and Equipment', 'Electric Parts Equ', 'Motor Parts')").groupBy($"gender").agg(format_number(sum($"amount") / countDistinct($"client_id"), 2).as("hardware_electric")), "gender").join(
	transactionsWithClients.filter("merch_name = 'Rugby and Football'").groupBy($"gender").agg(format_number(sum($"amount") / countDistinct($"client_id"), 2).as("sports")), "gender").join(
	transactionsWithClients.filter("merch_name = 'Florists'").groupBy($"gender").agg(format_number(sum($"amount") / countDistinct($"client_id"), 2).as("florists")), "gender").show(100, false)

sqlContext.sql("""
	SELECT *
	FROM (SELECT gender, 
				 format_number(SUM(amount) / COUNT(DISTINCT clients.client_id), 2) AS womens_clothing
		  FROM transactions 
			   INNER JOIN clients 
			   ON transactions.client_id = clients.client_id
		  WHERE merch_name IN ('Donna Affascinante', 'Donna Elegante')
		  GROUP BY gender) AS T1
	FULL OUTER JOIN (SELECT gender, 
							format_number(SUM(amount) / COUNT(DISTINCT clients.client_id), 2) AS mens_clothing
					 FROM transactions 
						  INNER JOIN clients 
						  ON transactions.client_id = clients.client_id
					 WHERE merch_name = 'Uomo Elegante'
					 GROUP BY gender) AS T2 ON T1.gender = T2.gender
	FULL OUTER JOIN (SELECT gender, 
							format_number(SUM(amount) / COUNT(DISTINCT clients.client_id), 2) AS cosmetics
					 FROM transactions 
						  INNER JOIN clients 
						  ON transactions.client_id = clients.client_id
					 WHERE merch_name = 'Cosmetic Stores'
					 GROUP BY gender) AS T3 ON T1.gender = T3.gender
	FULL OUTER JOIN (SELECT gender, 
							format_number(SUM(amount) / COUNT(DISTINCT clients.client_id), 2) AS florists
					 FROM transactions 
						  INNER JOIN clients 
						  ON transactions.client_id = clients.client_id
					 WHERE merch_name = 'Florists'
					 GROUP BY gender) AS T4 ON T1.gender = T4.gender
	FULL OUTER JOIN (SELECT gender, 
							format_number(SUM(amount) / COUNT(DISTINCT clients.client_id), 2) AS hardware_electric
					 FROM transactions 
						  INNER JOIN clients 
						  ON transactions.client_id = clients.client_id
					 WHERE merch_name IN ('Hardware and Equipment', 'Electric Parts Equ', 'Motor Parts')
					 GROUP BY gender) AS T5 ON T1.gender = T5.gender
	FULL OUTER JOIN (SELECT gender, 
							format_number(SUM(amount) / COUNT(DISTINCT clients.client_id), 2) AS sports
					 FROM transactions 
						  INNER JOIN clients 
						  ON transactions.client_id = clients.client_id
					 WHERE merch_name = 'Rugby and Football'
					 GROUP BY gender) AS T6 ON T1.gender = T6.gender
	ORDER BY T1.gender
""").show(100, false)
