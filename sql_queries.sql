/* All of SQL Language was from SQL Lite syntax*/

-- As says 7 days of information I will calculate the avg with those 7 days. Because I don't have information of entire week
-- For futere could extract more information from others stations to see which has more data
SELECT ROUND(AVG(fo.Num_Temperature), 2) FROM Fact_Observations fo;

SELECT 
    t1.Id_Observation,
    t1.Date_Time,
    t1.Num_WindSpeed AS current_wind_speed,
    t2.Num_WindSpeed AS next_wind_speed,
    t1.Num_WindSpeed + t2.Num_WindSpeed AS sum_wind_speed
FROM 
    Fact_Observations t1
JOIN 
    Fact_Observations t2
ON 
    t1.Date_Time < t2.Date_Time
WHERE 
    t2.Date_Time = (SELECT MIN(t3.Date_Time) 
                    FROM Fact_Observations t3 
                    WHERE t3.Date_Time > t1.Date_Time)
ORDER BY 
    sum_wind_speed DESC
LIMIT 1;