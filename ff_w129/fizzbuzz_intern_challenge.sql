-- BASIC SQL
WITH numbers AS (
    SELECT
        SEQ4()+1 as rn
    FROM
        TABLE(GENERATOR(ROWCOUNT => 100)) t
    )
SELECT
    CASE 
        WHEN MOD(rn, 3) = 0 AND MOD(rn, 5) = 0 THEN 'FizzBuzz'
        WHEN MOD(rn, 3) = 0 THEN 'Fizz'
        WHEN MOD(rn, 5) = 0 THEN 'Buzz'
        ELSE rn::STRING
    END AS FIZZBUZZ,
    CASE 
        WHEN rn%3 = 0 AND rn%5 = 0 THEN 'FizzBuzz'
        WHEN rn%3 = 0 THEN 'Fizz'
        WHEN rn%5 = 0 THEN 'Buzz'
        ELSE rn::STRING
    END AS FIZZBUZZ_2,
    CASE 
        WHEN (rn / 3 = FLOOR(rn / 3)) AND (rn / 5 = FLOOR(rn / 5)) THEN 'FizzBuzz'
        WHEN (rn / 3 = FLOOR(rn / 3)) THEN 'Fizz'
        WHEN (rn / 5 = FLOOR(rn / 5)) THEN 'Buzz'
        ELSE rn::STRING
    END AS FIZZBUZZ_3,
FROM
    numbers;

    
--Snowflake CORTEX
WITH data AS (
    SELECT
    PARSE_JSON(SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b','Generate JSON list that outputs numbers from 1 to 100, but with a twist:
            For multiples of 3, output “Fizz” instead of the number.
            For multiples of 5, output “Buzz” instead of the number.
            For multiples of both 3 and 5, output “FizzBuzz”. Without explanation and without ```json . ```')) as FIZZBUZZ_JSON
)
SELECT 
    VALUE::STRING AS FIZZBUZZ_4
FROM data, 
LATERAL FLATTEN(input => FIZZBUZZ_JSON);
