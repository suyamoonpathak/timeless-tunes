-- Create table for song_data.csv
CREATE EXTERNAL TABLE IF NOT EXISTS song_data (
    song_id STRING,
    title STRING,
    release STRING,
    artist_name STRING,
    year INT
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Create table for triplets_file.csv
CREATE EXTERNAL TABLE IF NOT EXISTS triplets_data (
    user_id STRING,
    song_id STRING,
    listen_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
