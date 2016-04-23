## Bureau of Labor Statistics Scraper

Python script to scrape state level unemployment and consumer price index data
from Bureau of Labor Statistics ftp (http://download.bls.gov/pub/time.series/)
and ingest data into Postgres/Greenplum/HAWQ database

1. Set database connection details in config.ini file -> works with postgres, greenplum and hawq
2. Run script
  ```bash
  ./bls-scraper.py
  ```

Link
http://download.bls.gov/pub/time.series/la/
