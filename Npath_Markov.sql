

    %connect Vantage-LIVE, hidewarnings=true



    %lsconnect



    %connect  AWS_PROD



    database ADLDEMOPRD3_tb250045



    DROP TABLE all_simple_path

    CREATE TABLE all_simple_path(
        cust_id INTEGER
        ,date_tb DATE FORMAT 'DD/MM/YYYY'
        ,event VARCHAR(10)
    )primary index(cust_id)



    INSERT INTO all_simple_path VALUES(1,'21/06/2022', 'start');
    INSERT INTO all_simple_path VALUES(1,'22/06/2022', 'c1');
    INSERT INTO all_simple_path VALUES(1,'23/06/2022', 'c3');
    INSERT INTO all_simple_path VALUES(1,'24/06/2022', 'conversion');
    
    INSERT INTO all_simple_path VALUES(2,'21/06/2022', 'start');
    INSERT INTO all_simple_path VALUES(2,'22/06/2022', 'c1');
    INSERT INTO all_simple_path VALUES(2,'23/06/2022', 'c3');
    INSERT INTO all_simple_path VALUES(2,'24/06/2022', 'c4');
    
    INSERT INTO all_simple_path VALUES(5,'21/06/2022', 'start');
    INSERT INTO all_simple_path VALUES(5,'22/06/2022', 'c1');
    INSERT INTO all_simple_path VALUES(5,'24/06/2022', 'c4');
    INSERT INTO all_simple_path VALUES(5,'25/06/2022', 'conversion');
    
    INSERT INTO all_simple_path VALUES(3,'21/06/2022', 'start');
    INSERT INTO all_simple_path VALUES(3,'22/06/2022', 'c2');
    INSERT INTO all_simple_path VALUES(4,'21/06/2022', 'start');
    INSERT INTO all_simple_path VALUES(4,'22/06/2022', 'c2');



    sel * from all_simple_path order by 1,2



    drop table all_path



    --Check all the journeys
    CREATE table all_path AS(
    SELECT * FROM nPath (
    ON all_simple_path
      PARTITION BY cust_id
      ORDER BY date_tb
      USING
        Symbols (TRUE AS A)
        Pattern ('A*')
        Mode (NONOVERLAPPING)
        Result (
          FIRST (cust_id OF A) AS cust_id,
          LAST (event OF A) AS last_event,
          ACCUMULATE (event OF A) AS page_path
        )
    ) AS dt2
    ) with data primary index(cust_id)



    SEL * FROM all_path order by cust_id



    DROP TABLE pairwise_transistions


    --Get the pairwise transistions and mark journeys with and outcome.
    CREATE TABLE pairwise_transistions as(
        SELECT dt2.page_path
              ,dt3.last_event
        FROM nPath (
                ON all_simple_path
                  PARTITION BY cust_id
                  ORDER BY date_tb
                  USING
                    Symbols (TRUE AS A, TRUE AS B)
                    Pattern ('A.B')
                    Mode (OVERLAPPING)
                    Result (
                      FIRST (cust_id OF A) AS cust_id,
                      ACCUMULATE (event OF ANY(A,B)) AS page_path
                    )
                ) AS dt2
        LEFT JOIN 
                all_path as dt3
        ON dt2.cust_id = dt3.cust_id
    ) WITH DATA PRIMARY INDEX(page_path)



    --Check pairwise transistions
    sel * from pairwise_transistions order by page_path



    --Main program that implements Laplacian smothing and log odds ratios
    CREATE TABLE log_odds_example as(
    SEL page_path
        ,prob_in_success
        ,prob_in_non_success
        ,ROUND(LOG(prob_in_success/prob_in_non_success),2) as log_odds
    FROM(
        SEL COALESCE(a.page_path,b.page_path) as page_path
        ,COALESCE(N_success, 0) as N_success_tb
        ,COALESCE(N_non_success, 0) as N_non_success_tb
        ,c.V
        ,sum(N_success_tb) over(order by a.page_path) as tot_success
        ,sum(N_non_success_tb) over(order by a.page_path) as tot_non_success
        --- Laplacian smothing
        ,(N_success_tb+1.00)/(tot_success+V) as prob_in_success
        ,(N_non_success_tb+1.00)/(tot_non_success+V) as prob_in_non_success
    FROM
    
        (SEL page_path , count(*) as N_success FROM pairwise_transistions where last_event = 'conversion' group by page_path) as a
    FULL JOIN 
        (SEL page_path , count(*) as N_non_success FROM pairwise_transistions where last_event <> 'conversion' group by page_path) as b
    ON a.page_path = b.page_path
    LEFT OUTER JOIN
        (SEL count(distinct page_path)as V  FROM pairwise_transistions ) as c
    ON 1=1
    ) as d
    ) with data primary index(page_path);


    SEL * FROM log_odds_example order by page_path



    DROP TABLE new_journey;



    -- Scoring new journey. 
    -- Note I will skip N_path to get pair wise transistions for a new journey. 
    -- The code is the same as for creating a model
    CREATE TABLE new_journey(
        page_path VARCHAR(20)
        ,cust_id INTEGER
    )primary index(cust_id)


    INSERT INTO new_journey VALUES('[start, c1]',6);
    INSERT INTO new_journey VALUES('[c1, c3]',6);
    INSERT INTO new_journey VALUES('[c3, c4]',6);

 

    SEL * FROM new_journey



    --Score a journey 
    SEL a.*,b.*
       ,ROUND(sum(b.log_odds) over(order by a.page_path),2) AS log_likelyhood_to_convert
    FROM 
        new_journey as a
    left join
        log_odds_example as b
    ON a.page_path = b.page_path

