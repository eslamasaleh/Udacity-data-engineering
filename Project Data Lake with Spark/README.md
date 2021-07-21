### Project: Data Lake
###### Note: folder dl.cfg content ```AWS_ACCESS_KEY_ID``` and ```AWS_SECRET_ACCESS_KEY``` need to be added 

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.
### Song Dataset
Song data: s3a://udacity-dend/song_data/*/*/*/*.json
Log data: s3a://udacity-dend/log-data/*.json

## Fact Table


### songplays 
- records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


## Dimension Tables

### users 
- users in the app
- user_id, first_name, last_name, gender, level

### songs 
- songs in music database
- song_id, title, artist_id, year, duration

### artists 
- artists in music database
- artist_id, name, location, lattitude, longitude

### time 
- timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday
# Deployment 



## Set Up EMR Cluster:

1. Log into the AWS console for Oregon and navigate to EMR
2. Click "Create Cluster"
3. Select "Go to advanced options"
4. Under "Software Configuration", select Hadoop, Hive, and Spark
5. Under "Edit software settings", enter the following configuration:
```[{"classification":"spark", "properties":{"maximizeResourceAllocation":"true"}, "configurations":[]}]```

6. Click "Next" at the bottom of the page to go to the "Hardware" page

7. I found some EC2 subnets do not work in the Oregon region (where the Udacity S3 data is us-west-2b)


8. You should only need a couple of worker instances (in addition to the master)


9. Click "Next" at the bottom of the page to go to the "General Options" page

10. Give your cluster a name and click "Next" at the bottom of the page

11. Pick your EC2 key pair in the drop-down.

12. Click "Create Cluster"

## Connect to Master Node and update the spark-env.sh file:
1. Open the port 22 in the security group of the EMR Cluster by opening EC2 then click on security group elastic group Master click on itthen inbound rules add rules choose SSH.
2. On the main page for your cluster in the AWS console, click on SSH next to "Master public DNS" On the Mac/Linux tab, copy the command to ssh into the master node. It should look roughly as follows:```ssh -i KEY_PAIR_FILE.pem hadoop@ec2-99-99-999-999.us-west-2.compute.amazonaws.com``` Paste it and run it in a BASH shell window and type "yes" when prompted
*** if you get an error use ```chmod 400 KEY_PAIR_FILE.pem```

3. Using sudo, append the following line to the /etc/spark/conf/spark-env.sh file: 
```export PYSPARK_PYTHON=/usr/bin/python3```

## Create a local tunnel to the EMR Spark History Server on your Linux machine:

1. Open up a new Bash shell and run the following command (using the proper IP for your master node):
```ssh -i KEY_PAIR_FILE.pem -N -L 8157:ec2-99-99-999-999.us-west-2.compute.amazonaws.com:18080 hadoop@ec2-99-99-999-999.us-west 2.compute.amazonaws.com```
2. Go to localhost:8157 in a web browser on your local machine and you should see the Spark History Server UI

## Run your job

1. SFTP the dl.cfg and etl.py files to the hadoop account directory on EMR:
```scp -i <.pem-file> <Local-Path> <username>@<EMR-MasterNode Endpoint>:~<EMR-path>```
2. In your home directory on the EMR master node (/home/hadoop), run the following command:
```spark-submit etl.py --master yarn --deploy-mode client --driver-memory 4g --num-executors 2 --executor-memory 2g --executor-core 2```
