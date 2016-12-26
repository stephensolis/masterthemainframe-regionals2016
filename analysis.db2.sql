-- *****************************************************************
-- ** PLEASE NOTE: The entire file takes about 50 seconds to run. **
-- *****************************************************************


-- *********************************************************************
-- ** Query #1: Transaction volume by day of month.                   **
-- ** Estimated running time: 3 sec.                                  **
-- ** Insights: There is a huge spike (nearly 4x) on the 2nd of every **
-- **           month. Businesses should be prepared for the          **
-- **           increased volume and use promotions to take           **
-- **           advantage.                                            **
-- *********************************************************************
SELECT DAY(ACAUREQ_HDR_CREDTT) AS day_of_month,
       DSN8.CURRENCY(SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT
                              AS NUMERIC(15, 3))), '$')
         AS total_tx_amount
FROM CARDUSR.SPPAYTB
     INNER JOIN CARDUSR.CLIENT_INFO
     ON CARDUSR.SPPAYTB.CONT_ID = CARDUSR.CLIENT_INFO.CONT_ID
GROUP BY DAY(ACAUREQ_HDR_CREDTT)
ORDER BY DAY(ACAUREQ_HDR_CREDTT);


-- *********************************************************************
-- ** Query #2: Transaction volume by weekday for movie transactions. **
-- ** Estimated running time: 3.5 sec.                                **
-- ** Insights: There is a large increase (as expected) on Saturday   **
-- **           and Sunday, but Fridays are surprisingly unpopular,   **
-- **           about the same as other weekdays. Promotional pricing **
-- **           for all weekdays may be effective.                    **
-- *********************************************************************
SELECT MIN(DSN8.DAYNAME(DATE(ACAUREQ_HDR_CREDTT AT TIME ZONE '+00:00')))
         AS weekday,
       DSN8.CURRENCY(SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT
                              AS NUMERIC(15, 3))), '$')
         AS total_tx_amount
FROM CARDUSR.SPPAYTB
     INNER JOIN CARDUSR.CLIENT_INFO
     ON CARDUSR.SPPAYTB.CONT_ID = CARDUSR.CLIENT_INFO.CONT_ID
WHERE ACAUREQ_AUREQ_ENV_M_CMONNM = 'Movies'
GROUP BY DAYOFWEEK_ISO(ACAUREQ_HDR_CREDTT)
ORDER BY DAYOFWEEK_ISO(ACAUREQ_HDR_CREDTT);


-- *********************************************************************
-- ** Query #3: Transaction volume per month for some merchant        **
-- **           categories.                                           **
-- ** Estimated running time: 28.5 sec.                               **
-- ** Insights: Transaction volume remains relatively consistent in   **
-- **           general. However, travel spending increases           **
-- **           dramatically in August and September -- these         **
-- **           merchants should be prepared for the increase and     **
-- **           consider promotions in other months. Clear seasonal   **
-- **           trends can also be seen for the heating/plumbing and  **
-- **           home furnishing categories.                           **
-- ** Note: Merchant category code information was taken from a       **
-- **       Citibank document: https://www.citibank.com/tts/          **
-- **       card_solutions/commercial_cards/site/docs/dod/            **
-- **       mcc_codes_0220.pdf                                        **
-- ** Note!: The result set is wider than 80 columns. Please scroll   **
-- **        right to see all the data.                               **
-- *********************************************************************
SELECT month,
       DSN8.CURRENCY(COALESCE(overall, 0.), '$')
         AS overall,
       DSN8.CURRENCY(COALESCE(travel, 0.), '$')
         AS travel,
       DSN8.CURRENCY(COALESCE(heating_and_plumbing, 0.), '$')
         AS heating_and_plumbing,
       DSN8.CURRENCY(COALESCE(home_furnishings, 0.), '$')
         AS home_furnishings
FROM (SELECT MONTH(ACAUREQ_HDR_CREDTT) AS month_num,
             MIN(DSN8.MONTHNAME(DATE(ACAUREQ_HDR_CREDTT
                                     AT TIME ZONE '+00:00'))) AS month,
             SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC(15, 3)))
               AS overall
      FROM CARDUSR.SPPAYTB
           INNER JOIN CARDUSR.CLIENT_INFO
           ON CARDUSR.SPPAYTB.CONT_ID = CARDUSR.CLIENT_INFO.CONT_ID
      GROUP BY MONTH(ACAUREQ_HDR_CREDTT)) AS T1
FULL JOIN (SELECT MONTH(ACAUREQ_HDR_CREDTT) AS month_num,
                  SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT
                           AS NUMERIC(15, 3)))
                    AS travel
           FROM CARDUSR.SPPAYTB
                INNER JOIN CARDUSR.CLIENT_INFO
                ON CARDUSR.SPPAYTB.CONT_ID = CARDUSR.CLIENT_INFO.CONT_ID
           WHERE ACAUREQ_AUREQ_TX_MRCHNTCTGYCD BETWEEN '3000' AND '3999'
           GROUP BY MONTH(ACAUREQ_HDR_CREDTT)) AS T2
  ON T1.month_num = T2.month_num
FULL JOIN (SELECT MONTH(ACAUREQ_HDR_CREDTT) AS month_num,
                  SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT
                           AS NUMERIC(15, 3)))
                    AS heating_and_plumbing
           FROM CARDUSR.SPPAYTB
                INNER JOIN CARDUSR.CLIENT_INFO
                ON CARDUSR.SPPAYTB.CONT_ID = CARDUSR.CLIENT_INFO.CONT_ID
           WHERE ACAUREQ_AUREQ_ENV_M_CMONNM = 'Plumbing and Heating'
           GROUP BY MONTH(ACAUREQ_HDR_CREDTT)) AS T3
  ON T1.month_num = T3.month_num
FULL JOIN (SELECT MONTH(ACAUREQ_HDR_CREDTT) AS month_num,
                  SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT
                           AS NUMERIC(15, 3)))
                    AS home_furnishings
           FROM CARDUSR.SPPAYTB
                INNER JOIN CARDUSR.CLIENT_INFO
                ON CARDUSR.SPPAYTB.CONT_ID = CARDUSR.CLIENT_INFO.CONT_ID
           WHERE ACAUREQ_AUREQ_ENV_M_CMONNM IN
             ('House of Drapery', 'Home of Crystal',
              'All for your House', 'My Beatiful House')
           GROUP BY MONTH(ACAUREQ_HDR_CREDTT)) AS T4
  ON T1.month_num = T4.month_num
ORDER BY T1.month_num;
-- *********************************
-- ** >>> Please scroll right >>> **
-- *********************************


-- *********************************************************************
-- ** Query #4: Some statistics by card brand.                        **
-- ** Estimated running time: 5 sec.                                  **
-- ** Insights: These statistics could help banks decide where to     **
-- **           focus marketing and promotion campaigns for their     **
-- **           credit and debit products.                            **
-- ** Note!: The result set is wider than 80 columns. Please scroll   **
-- **        right to see all the data.                               **
-- *********************************************************************
SELECT CAST(T1.card_brand AS VARCHAR(15)) as card_brand,
       DSN8.CURRENCY(avg_tx_amount, '$') AS avg_tx_amount,
       DSN8.CURRENCY(client_avg_tx_sum, '$') AS client_avg_tx_sum,
       CAST(VARCHAR_FORMAT(client_avg_tx_count, '9999990.0')
            AS VARCHAR(10))
         AS client_avg_tx_count,
       DSN8.CURRENCY(client_avg_income, '$') AS client_avg_income
FROM (SELECT ACAUREQ_AUREQ_ENV_C_CARDBRND AS card_brand,
             AVG(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC(15, 3)))
               AS avg_tx_amount,
             SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC(15, 3)))
               / COUNT(DISTINCT CARDUSR.CLIENT_INFO.CONT_ID)
               AS client_avg_tx_sum,
             CAST(COUNT(*) AS FLOAT)
               / COUNT(DISTINCT CARDUSR.CLIENT_INFO.CONT_ID)
               AS client_avg_tx_count
      FROM CARDUSR.SPPAYTB
           INNER JOIN CARDUSR.CLIENT_INFO
           ON CARDUSR.SPPAYTB.CONT_ID = CARDUSR.CLIENT_INFO.CONT_ID
      GROUP BY ACAUREQ_AUREQ_ENV_C_CARDBRND) AS T1
INNER JOIN (SELECT ACAUREQ_AUREQ_ENV_C_CARDBRND AS card_brand,
                   AVG(CAST(ANNUAL_INCOME AS NUMERIC(15, 3)))
                     AS client_avg_income
            FROM CARDUSR.CLIENT_INFO
                 INNER JOIN (SELECT DISTINCT CONT_ID,
                                            ACAUREQ_AUREQ_ENV_C_CARDBRND
                             FROM CARDUSR.SPPAYTB) AS CLIENT_CARDS
                 ON CARDUSR.CLIENT_INFO.CONT_ID = CLIENT_CARDS.CONT_ID
            GROUP BY ACAUREQ_AUREQ_ENV_C_CARDBRND) AS T2
  ON T1.card_brand = T2.card_brand
ORDER BY card_brand;
-- *********************************
-- ** >>> Please scroll right >>> **
-- *********************************


-- *********************************************************************
-- ** Query #5: Average transaction volume by gender for some         **
-- **           merchant categories.                                  **
-- ** Estimated running time: 11 sec.                                 **
-- ** Insights: The difference between spending by gender at          **
-- **           merchants traditionally considered to be patronized   **
-- **           primarily by one gender is smaller than might be      **
-- **           expected. This suggests gender-targeted marketing     **
-- **           might be of limited effectiveness.                    **
-- ** Note!: The result set is wider than 80 columns. Please scroll   **
-- **        right to see all the data.                               **
-- *********************************************************************
SELECT T1.GENDER,
       DSN8.CURRENCY(COALESCE(womens_clothing, 0.), '$')
         AS womens_clothing,
       DSN8.CURRENCY(COALESCE(mens_clothing, 0.), '$')
         AS mens_clothing,
       DSN8.CURRENCY(COALESCE(cosmetics, 0.), '$')
         AS cosmetics,
       DSN8.CURRENCY(COALESCE(florists, 0.), '$')
         AS florists,
       DSN8.CURRENCY(COALESCE(hardware_electric, 0.), '$')
         AS hardware_electric,
       DSN8.CURRENCY(COALESCE(sports, 0.), '$')
         AS sports
FROM (SELECT GENDER,
             SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT AS NUMERIC(15, 3)))
               / COUNT(DISTINCT CARDUSR.CLIENT_INFO.CONT_ID)
               AS womens_clothing
      FROM CARDUSR.SPPAYTB
           INNER JOIN CARDUSR.CLIENT_INFO
           ON CARDUSR.SPPAYTB.CONT_ID = CARDUSR.CLIENT_INFO.CONT_ID
      WHERE ACAUREQ_AUREQ_ENV_M_CMONNM IN
        ('Donna Affascinante', 'Donna Elegante')
      GROUP BY GENDER) AS T1
FULL JOIN (SELECT GENDER,
                  SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT
                           AS NUMERIC(15, 3)))
                    / COUNT(DISTINCT CARDUSR.CLIENT_INFO.CONT_ID)
                    AS mens_clothing
           FROM CARDUSR.SPPAYTB
                INNER JOIN CARDUSR.CLIENT_INFO
                ON CARDUSR.SPPAYTB.CONT_ID = CARDUSR.CLIENT_INFO.CONT_ID
           WHERE ACAUREQ_AUREQ_ENV_M_CMONNM = 'Uomo Elegante'
           GROUP BY GENDER) AS T2 ON T1.GENDER = T2.GENDER
FULL JOIN (SELECT GENDER,
                  SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT
                           AS NUMERIC(15, 3)))
                    / COUNT(DISTINCT CARDUSR.CLIENT_INFO.CONT_ID)
                    AS cosmetics
           FROM CARDUSR.SPPAYTB
                INNER JOIN CARDUSR.CLIENT_INFO
                ON CARDUSR.SPPAYTB.CONT_ID = CARDUSR.CLIENT_INFO.CONT_ID
           WHERE ACAUREQ_AUREQ_ENV_M_CMONNM = 'Cosmetic Stores'
           GROUP BY GENDER) AS T3 ON T1.GENDER = T3.GENDER
FULL JOIN (SELECT GENDER,
                  SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT
                           AS NUMERIC(15, 3)))
                    / COUNT(DISTINCT CARDUSR.CLIENT_INFO.CONT_ID)
                    AS florists
           FROM CARDUSR.SPPAYTB
                INNER JOIN CARDUSR.CLIENT_INFO
                ON CARDUSR.SPPAYTB.CONT_ID = CARDUSR.CLIENT_INFO.CONT_ID
           WHERE ACAUREQ_AUREQ_ENV_M_CMONNM = 'Florists'
           GROUP BY GENDER) AS T4 ON T1.GENDER = T4.GENDER
FULL JOIN (SELECT GENDER,
                  SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT
                           AS NUMERIC(15, 3)))
                    / COUNT(DISTINCT CARDUSR.CLIENT_INFO.CONT_ID)
                    AS hardware_electric
           FROM CARDUSR.SPPAYTB
                INNER JOIN CARDUSR.CLIENT_INFO
                ON CARDUSR.SPPAYTB.CONT_ID = CARDUSR.CLIENT_INFO.CONT_ID
           WHERE ACAUREQ_AUREQ_ENV_M_CMONNM IN
                   ('Hardware and Equipment', 'Electric Parts Equ',
                    'Motor Parts')
                 GROUP BY GENDER) AS T5 ON T1.GENDER = T5.GENDER
FULL JOIN (SELECT GENDER,
                  SUM(CAST(ACAUREQ_AUREQ_TX_DT_TTLAMT
                           AS NUMERIC(15, 3)))
                    / COUNT(DISTINCT CARDUSR.CLIENT_INFO.CONT_ID)
                    AS sports
           FROM CARDUSR.SPPAYTB
                INNER JOIN CARDUSR.CLIENT_INFO
                ON CARDUSR.SPPAYTB.CONT_ID = CARDUSR.CLIENT_INFO.CONT_ID
           WHERE ACAUREQ_AUREQ_ENV_M_CMONNM = 'Rugby and Football'
           GROUP BY GENDER) AS T6 ON T1.GENDER = T6.GENDER
ORDER BY GENDER;
-- *********************************
-- ** >>> Please scroll right >>> **
-- *********************************
