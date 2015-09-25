import sys, time, datetime

def main(task, *args):

    print ("time: %s" % str(datetime.datetime.now()))
    print ("cpubound_process file")
    print (task)
    for i in range(5):
        time.sleep(2)
        print ("process sleep: ", i)

if __name__ == '__main__':
    main('test task')


# exit(0)
