import sys, time, datetime

def main(task, *args):

    with open('test_process.log', 'w+') as f:
        print ("time: %s" % str(datetime.datetime.now()), file=f)
        print ("cpubound_process file", file=f)
        print (task)
        for i in range(5):
            time.sleep(2)
            print ("process sleep: ", i, file=f)

if __name__ == '__main__':
    main('test task')


# exit(0)
