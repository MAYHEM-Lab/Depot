import time, sys, os
millis = int(round(time.time() * 1000))
custom_message = "This message is from {0}".format(sys.argv[1])
ets = millis

for x in range(len(sys.argv)):
	if (x == 2):
		custom_message = sys.argv[2]
	if (x == 3):
		ets = sys.argv[3]

for i in range(1,12):
	message = """{{"name":"{0}","message": "{2}","ets": {1}}}""".format(sys.argv[1], ets, custom_message)
	print(message)
	bashCommand = "echo '{0}' | $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic messenger $1".format(message)
	os.system(bashCommand)
	print("message sent!")
	time.sleep(5)