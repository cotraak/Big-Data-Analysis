# Big Data Analysis

Permissions need to be granted to run the mapper and reducer files. And that can be done using the following command

chmod 755 wmapper.py wreducer.py

Input is the 19years (2000-2019) weather dataset which is located at /user/tatavag/weather

Command to execute the program on the Hadoop Server:

hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar -file /home/malyalaa/wmapper.py -mapper /home/malyalaa/wmapper.py -file /home/malyalaa/wreducer.py -reducer /home/malyalaa/wreducer.py -input /user/tatavag/weather -output /user/malyalaa/out2

After the job is successfully done, it notifies the user by displaying the output directory

Output directory is /user/malyalaa/out2

Command to list the files present in the output directory:

hadoop fs -ls /user/malyalaa/out2

Output file is /user/malyalaa/out2/part-00000

It can be displayed using the below command:

hdfs dfs -cat /user/malyalaa/out2/part-00000

Also, the output can be copied into another text file using the below command:

hdfs dfs -cat /user/malyalaa/out2/part-00000 >> p2_out.txt
