SELECT EXTRACT(DAY FROM CAST(ACAUREQ_HDR_CREDTT AS TIMESTAMP)) AS day_of_month,
       SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS MONEY)) AS total_tx_amount
FROM CARDUSR.SPPAYTB
     INNER JOIN CARDUSR.CLIENT_INFO USING (CONT_ID)
GROUP BY day_of_month
ORDER BY day_of_month;


SELECT TO_CHAR(CAST(ACAUREQ_HDR_CREDTT AS TIMESTAMP), 'Day') AS weekday,
       SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS MONEY)) AS total_tx_amount
FROM CARDUSR.SPPAYTB
     INNER JOIN CARDUSR.CLIENT_INFO USING (CONT_ID)
WHERE ACAUREQ_AUREQ_ENV_M_CMONNM = 'Movies'
GROUP BY weekday
ORDER BY MAX(EXTRACT(ISODOW FROM CAST(ACAUREQ_HDR_CREDTT AS TIMESTAMP)));


SELECT month,
       CAST(COALESCE(overall, 0.) AS MONEY) AS overall,
       CAST(COALESCE(travel, 0.) AS MONEY) AS travel,
       CAST(COALESCE(heating_and_plumbing, 0.) AS MONEY) AS heating_and_plumbing,
       CAST(COALESCE(home_furnishings, 0.) AS MONEY) AS home_furnishings
FROM (SELECT TO_CHAR(CAST(ACAUREQ_HDR_CREDTT AS TIMESTAMP), 'Month') AS month,
             MAX(EXTRACT(MONTH FROM CAST(ACAUREQ_HDR_CREDTT AS TIMESTAMP))) AS month_num,
             SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC)) AS overall
      FROM CARDUSR.SPPAYTB
           INNER JOIN CARDUSR.CLIENT_INFO USING (CONT_ID)
      GROUP BY month) AS T1
FULL OUTER JOIN (SELECT TO_CHAR(CAST(ACAUREQ_HDR_CREDTT AS TIMESTAMP), 'Month') AS month,
                        SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC)) AS travel
                 FROM CARDUSR.SPPAYTB
                      INNER JOIN CARDUSR.CLIENT_INFO USING (CONT_ID)
                 WHERE ACAUREQ_AUREQ_TX_MRCHNTCTGYCD BETWEEN '3000' AND '3999'
                 GROUP BY month) AS T2 USING (month)
FULL OUTER JOIN (SELECT TO_CHAR(CAST(ACAUREQ_HDR_CREDTT AS TIMESTAMP), 'Month') AS month,
                        SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC)) AS heating_and_plumbing
                 FROM CARDUSR.SPPAYTB
                      INNER JOIN CARDUSR.CLIENT_INFO USING (CONT_ID)
                 WHERE ACAUREQ_AUREQ_ENV_M_CMONNM = 'Plumbing and Heating'
                 GROUP BY month) AS T3 USING (month)
FULL OUTER JOIN (SELECT TO_CHAR(CAST(ACAUREQ_HDR_CREDTT AS TIMESTAMP), 'Month') AS month,
                        SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC)) AS home_furnishings
                 FROM CARDUSR.SPPAYTB
                      INNER JOIN CARDUSR.CLIENT_INFO USING (CONT_ID)
                 WHERE ACAUREQ_AUREQ_ENV_M_CMONNM IN
                   ('House of Drapery', 'Home of Crystal',
                    'All for your House', 'My Beatiful House')
                 GROUP BY month) AS T4 USING (month)
ORDER BY month_num;


SELECT card_brand,
       CAST(avg_tx_amount AS MONEY) AS avg_tx_amount,
       CAST(client_avg_tx_sum AS MONEY) AS client_avg_tx_sum,
       ROUND(client_avg_tx_count, 1) AS client_avg_tx_count,
       CAST(client_avg_income AS MONEY) AS client_avg_income
FROM (SELECT ACAUREQ_AUREQ_ENV_C_CARDBRND AS card_brand,
             AVG(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC)) AS avg_tx_amount,
             SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC))
               / COUNT(DISTINCT CONT_ID) AS client_avg_tx_sum,
             CAST(COUNT(*) AS NUMERIC)
               / COUNT(DISTINCT CONT_ID) AS client_avg_tx_count
      FROM CARDUSR.SPPAYTB
           INNER JOIN CARDUSR.CLIENT_INFO USING (CONT_ID)
      GROUP BY card_brand) AS T1
INNER JOIN (SELECT ACAUREQ_AUREQ_ENV_C_CARDBRND AS card_brand,
                   AVG(CAST(ANNUAL_INCOME AS NUMERIC)) AS client_avg_income
            FROM CARDUSR.CLIENT_INFO
                 INNER JOIN (SELECT DISTINCT CONT_ID,
                                             ACAUREQ_AUREQ_ENV_C_CARDBRND
                             FROM CARDUSR.SPPAYTB) AS CLIENT_CARDS USING (CONT_ID)
            GROUP BY card_brand) AS T2 USING (card_brand)
ORDER BY card_brand;


SELECT GENDER as gender,
       CAST(COALESCE(womens_clothing, 0.) AS MONEY) AS womens_clothing,
       CAST(COALESCE(mens_clothing, 0.) AS MONEY) AS mens_clothing,
       CAST(COALESCE(cosmetics, 0.) AS MONEY) AS cosmetics,
       CAST(COALESCE(florists, 0.) AS MONEY) AS florists,
       CAST(COALESCE(hardware_electric, 0.) AS MONEY) AS hardware_electric,
       CAST(COALESCE(sports, 0.) AS MONEY) AS sports
FROM (SELECT GENDER,
             SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC))
               / COUNT(DISTINCT CONT_ID) AS womens_clothing
      FROM CARDUSR.SPPAYTB
           INNER JOIN CARDUSR.CLIENT_INFO USING (CONT_ID)
      WHERE ACAUREQ_AUREQ_ENV_M_CMONNM IN
        ('Donna Affascinante', 'Donna Elegante')
      GROUP BY GENDER) AS T1
FULL OUTER JOIN (SELECT GENDER,
                        SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC))
                          / COUNT(DISTINCT CONT_ID) AS mens_clothing
                 FROM CARDUSR.SPPAYTB
                      INNER JOIN CARDUSR.CLIENT_INFO USING (CONT_ID)
                 WHERE ACAUREQ_AUREQ_ENV_M_CMONNM = 'Uomo Elegante'
                 GROUP BY GENDER) AS T2 USING (GENDER)
FULL OUTER JOIN (SELECT GENDER,
                        SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC))
                          / COUNT(DISTINCT CONT_ID) AS cosmetics
                 FROM CARDUSR.SPPAYTB
                      INNER JOIN CARDUSR.CLIENT_INFO USING (CONT_ID)
                 WHERE ACAUREQ_AUREQ_ENV_M_CMONNM = 'Cosmetic Stores'
                 GROUP BY GENDER) AS T3 USING (GENDER)
FULL OUTER JOIN (SELECT GENDER,
                        SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC))
                          / COUNT(DISTINCT CONT_ID) AS florists
                 FROM CARDUSR.SPPAYTB
                      INNER JOIN CARDUSR.CLIENT_INFO USING (CONT_ID)
                 WHERE ACAUREQ_AUREQ_ENV_M_CMONNM = 'Florists'
                 GROUP BY GENDER) AS T4 USING (GENDER)
FULL OUTER JOIN (SELECT GENDER,
                        SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC))
                          / COUNT(DISTINCT CONT_ID) AS hardware_electric
                 FROM CARDUSR.SPPAYTB
                      INNER JOIN CARDUSR.CLIENT_INFO USING (CONT_ID)
                 WHERE ACAUREQ_AUREQ_ENV_M_CMONNM IN
                   ('Hardware and Equipment', 'Electric Parts Equ', 'Motor Parts')
                 GROUP BY GENDER) AS T5 USING (GENDER)
FULL OUTER JOIN (SELECT GENDER,
                        SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC))
                          / COUNT(DISTINCT CONT_ID) AS sports
                 FROM CARDUSR.SPPAYTB
                      INNER JOIN CARDUSR.CLIENT_INFO USING (CONT_ID)
                 WHERE ACAUREQ_AUREQ_ENV_M_CMONNM = 'Rugby and Football'
                 GROUP BY GENDER) AS T6 USING (GENDER)
ORDER BY gender;
