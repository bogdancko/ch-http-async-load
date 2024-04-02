import random
from datetime import datetime
import aiohttp
import asyncio
import time
import re

start_time = time.time()

# Input request count
request_count = int(input("Request count: "))

# Clickhouse url:port
url = 'http://localhost:18123'

# OnTime https://clickhouse.com/docs/en/getting-started/example-datasets/ontime

# Q0 209.96 million rows
q0 = """
SELECT avg(c1)
FROM
(
    SELECT Year, Month, count(*) AS c1
    FROM ontime
    GROUP BY Year, Month
)
ORDER BY c DESC
FORMAT Null;
"""

# Q1. The number of flights per day from the year 2000 to 2012 84.37 million rows
q1 = """
SELECT DayOfWeek, count(*) AS c
FROM ontime
WHERE Year>=2012 AND Year<=2012
GROUP BY DayOfWeek
ORDER BY c DESC
FORMAT Null;
"""

# Q2. The number of flights delayed by more than 10 minutes, grouped by the day of the week, for 2000-2009 59.29 million rows
q2 = """
SELECT DayOfWeek, count(*) AS c
FROM ontime
WHERE DepDelay>10 AND Year>=2009 AND Year<=2009
GROUP BY DayOfWeek
ORDER BY c DESC
FORMAT Null;
"""

# Q3. The number of delays by the airport for 2000-2010 59.29 million rows
q3 = """
SELECT Origin, count(*) AS c
FROM ontime
WHERE DepDelay>10 AND Year>=2010 AND Year<=2010
GROUP BY Origin
ORDER BY c DESC
LIMIT 10
FORMAT Null;
"""

# Q4. The number of delays by carrier for 2007 7.46 million rows
q4 = """
SELECT IATA_CODE_Reporting_Airline AS Carrier, count(*)
FROM ontime
WHERE DepDelay>10 AND Year=2007
GROUP BY Carrier
ORDER BY count(*) DESC
FORMAT Null;
"""

# Q6. The previous request for a broader range of years, 2010-2020 134.51 million rows
q6 = """
SELECT Carrier, c, c2, c*100/c2 as c3
FROM
(
    SELECT
        IATA_CODE_Reporting_Airline AS Carrier,
        count(*) AS c
    FROM ontime
    WHERE DepDelay>10
        AND Year>=2020 AND Year<=2020
    GROUP BY Carrier
) q
JOIN
(
    SELECT
        IATA_CODE_Reporting_Airline AS Carrier,
        count(*) AS c2
    FROM ontime
    WHERE Year>=2020 AND Year<=2020
    GROUP BY Carrier
) qq USING Carrier
ORDER BY c3 DESC
FORMAT Null;
"""

# Q10. 168.15 million rows
q10 = """
SELECT
   min(Year), max(Year), IATA_CODE_Reporting_Airline AS Carrier, count(*) AS cnt,
   sum(ArrDelayMinutes>30) AS flights_delayed,
   round(sum(ArrDelayMinutes>30)/count(*),2) AS rate
FROM ontime
WHERE
   DayOfWeek NOT IN (6,7) AND OriginState NOT IN ('AK', 'HI', 'PR', 'VI')
   AND DestState NOT IN ('AK', 'HI', 'PR', 'VI')
   AND FlightDate < '2010-01-01'
GROUP by Carrier
HAVING cnt>100000 and max(Year)>1990
ORDER by rate DESC
LIMIT 1000
FORMAT Null;
"""

# Brown University Benchmark https://clickhouse.com/docs/en/getting-started/example-datasets/brown-benchmark

# -- Q1.1: What is the CPU/network utilization for each web server since midnight?
qbub1 = """
SELECT machine_name,
       MIN(cpu) AS cpu_min,
       MAX(cpu) AS cpu_max,
       AVG(cpu) AS cpu_avg,
       MIN(net_in) AS net_in_min,
       MAX(net_in) AS net_in_max,
       AVG(net_in) AS net_in_avg,
       MIN(net_out) AS net_out_min,
       MAX(net_out) AS net_out_max,
       AVG(net_out) AS net_out_avg
FROM (
  SELECT machine_name,
         COALESCE(cpu_user, 0.0) AS cpu,
         COALESCE(bytes_in, 0.0) AS net_in,
         COALESCE(bytes_out, 0.0) AS net_out
  FROM mgbench.logs1
  WHERE machine_name IN ('anansi','aragog','urd')
    AND log_time >= TIMESTAMP '2017-01-11 00:00:00'
) AS r
GROUP BY machine_name
FORMAT Null;
"""

# -- Q1.2: Which computer lab machines have been offline in the past day?
qbub2 = """
SELECT machine_name,
       log_time
FROM mgbench.logs1
WHERE (machine_name LIKE 'cslab%' OR
       machine_name LIKE 'mslab%')
  AND load_one IS NULL
  AND log_time >= TIMESTAMP '2017-01-10 00:00:00'
ORDER BY machine_name,
         log_time
FORMAT Null;
"""

# -- Q1.4: Over 1 month, how often was each server blocked on disk I/O?
qbub4 = """
SELECT machine_name,
       COUNT(*) AS spikes
FROM mgbench.logs1
WHERE machine_group = 'Servers'
  AND cpu_wio > 0.99
  AND log_time >= TIMESTAMP '2016-12-01 00:00:00'
  AND log_time < TIMESTAMP '2017-01-01 00:00:00'
GROUP BY machine_name
ORDER BY spikes DESC
LIMIT 10
FORMAT Null;
"""

# -- Q1.5: Which externally reachable VMs have run low on memory?
qbub5 = """
SELECT machine_name,
       dt,
       MIN(mem_free) AS mem_free_min
FROM (
  SELECT machine_name,
         CAST(log_time AS DATE) AS dt,
         mem_free
  FROM mgbench.logs1
  WHERE machine_group = 'DMZ'
    AND mem_free IS NOT NULL
) AS r
GROUP BY machine_name,
         dt
HAVING MIN(mem_free) < 10000
ORDER BY machine_name,
         dt
FORMAT Null;
"""

# List of queries for request
# query_list = [q1, q2, q3, q4, q6]
query_list = [qbub1, qbub2, qbub4]


def extract_elapsed_time(raw_string):
    """Extract "elapsed_ns" from Clickhouse output

    Keyword arguments:
    raw_string -- Clickhouse output string
    """
    m = re.search(pattern='elapsed_ns\":\"(?P<elapsed_ns>.+?)\"', string=str(raw_string))
    if m:
        return m.group('elapsed_ns')
    else:
        return 'null'


def extract_total_rows_to_read(raw_string):
    """Extract "total_rows_to_read" from Clickhouse output

    Keyword arguments:
    raw_string -- Clickhouse output string
    """
    m = re.search(pattern='"total_rows_to_read":"(?P<total_rows_to_read>.+?)\"', string=raw_string)
    if m:
        return m.group('total_rows_to_read')
    else:
        return 'null'


def nanosec2sec(ns):
    """From nanosececonds (ns) to seconds (s)

    Keyword arguments:
    ns -- nanoseconds
    """
    sec = round(int(ns) / 1000000000, 5)
    return sec


def mln(x):
    """Long millions to short form

    Keyword arguments:
    x -- millions
    """
    mln = round(int(x) / 1000000, 2)
    return mln


async def do_magic(session, url, query):
    async with session.get(url, params={'query': random.choice(query_list)}) as resp:
        pokemon = await resp.text()
        return resp.json


async def main():
    async with aiohttp.ClientSession() as session:

        tasks = []
        for number in range(request_count):
            tasks.append(asyncio.ensure_future(do_magic(session, url, random.choice(query_list))))

        jobs = await asyncio.gather(*tasks)
        for job in jobs:
            elapsed_ns = nanosec2sec(extract_elapsed_time(str(job)))
            total_rows_to_read = mln(extract_total_rows_to_read(str(job)))
            ts = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
            filename = str(request_count) + '-requests.csv'
            # filename = ts + '-text_file.txt'
            with open(filename, 'a') as f:
                # f.write(f'%s, %s\n' % (number, elapsed_ns))
                f.write(f'%s, %s\n' % (elapsed_ns, total_rows_to_read))
            print(f'%s, %s\n' % (elapsed_ns, total_rows_to_read))
            # print(pokemon)
            # print(f'%s, %s\n' % (number, elapsed_ns))


asyncio.run(main())
print("--- %s seconds ---" % (time.time() - start_time))
