from com.datasynapse.fabric.admin.info import AllocationInfo
from com.datasynapse.fabric.util import GridlibUtils, ContainerUtils
from com.datasynapse.fabric.common import RuntimeContextVariable, ActivationInfo
from com.datasynapse.fabric.admin.info import ComponentAllocationInfo
from subprocess import PIPE
import os
import time
import socket
import sys
import threading
import logging

sys.path.append(proxy.getContainer().getScript(0).getFile().getParentFile().getAbsolutePath())
ContainerUtils.getLogger(proxy).info("sys.path modified: " + str(sys.path) )
from hadoop_enabler_common import *

def doInit(additionalVariables):

    ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Beginning doInit()")

    doInit_common(additionalVariables)        
    
    additionalVariables.add(RuntimeContextVariable("hadoop_enabler_balancer_BALANCER_RECEIVED_REQUESTS", "", 
                                                   RuntimeContextVariable.STRING_TYPE))

    additionalVariables.add(RuntimeContextVariable("hadoop_enabler_balancer_BALANCER_NEW_REQUEST_FLAG", "False", 
                                                   RuntimeContextVariable.STRING_TYPE))

    additionalVariables.add(RuntimeContextVariable("hadoop_enabler_balancer_BALANCER_EXIT_FLAG", "False", 
                                                   RuntimeContextVariable.STRING_TYPE))
    
    """
    Create New Thread objects for processing requests and save for later
    """
    logger = ContainerUtils.getLogger(proxy)
    slstdout = StreamToLogger(logger,"STDOUT")
    slstderr = StreamToLogger(logger,"STDERR")
    lock = threading.Lock()
    processReqThread = processRequestsThread(slstdout, slstderr, lock)
    additionalVariables.add(RuntimeContextVariable("hadoop_enabler_balancer_BALANCER_PROCESS_REQUEST_THREAD", processReqThread, 
                                                   RuntimeContextVariable.OBJECT_TYPE))
    
    checkForReqThread = checkForRequestsThread(slstdout, slstderr, lock)
    additionalVariables.add(RuntimeContextVariable("hadoop_enabler_balancer_BALANCER_CHECKFOR_REQUEST_THREAD", checkForReqThread, 
                                                   RuntimeContextVariable.OBJECT_TYPE))

    proxy.doInit(additionalVariables)
    
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Exiting doInit()")


def doStart():
    
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Beginning doStart()")
    
    hadoop_home_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_HOME_DIR').getValue()
    
    doStart_common()                 

    processRequetsThread = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_balancer_BALANCER_PROCESS_REQUEST_THREAD').getValue()
    processRequetsThread.start()
    
    startupdelay = 0
    if (True):   #TODO  Base decision to delay on whether Namenode just started or has been running for a while 
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Hadoop cluster is just starting.  Waiting [" + str(startupdelay) + 
                                             "] seconds before processing any balancer requests so more datanodes can join.")
    else:
        startupdelay = 0
        
    time.sleep(startupdelay)
    
    checkForReqThread = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_balancer_BALANCER_CHECKFOR_REQUEST_THREAD').getValue()
    checkForReqThread.start()
    
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Exiting doStart()")

 
def doShutdown():

    ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Beginning doShutdown()")

    proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_balancer_BALANCER_EXIT_FLAG').setValue("True")
    
    stopBalancer()
    
    processRequetsThread = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_balancer_BALANCER_PROCESS_REQUEST_THREAD').getValue()
    if processRequetsThread.isAlive():
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Waiting for Processing thread to exit.")
        processRequetsThread.join()

    checkForReqThread = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_balancer_BALANCER_CHECKFOR_REQUEST_THREAD').getValue()
    if checkForReqThread.isAlive():
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Waiting for Queue monitor thread to exit.")
        checkForReqThread.join()
    
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Exiting doShutdown()")
    
def hasContainerStarted():
    status = True    
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer - hasContainerStarted()]  Return status is: " + str(status))
    return status

def isContainerRunning():

    processRequetsThread = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_balancer_BALANCER_PROCESS_REQUEST_THREAD').getValue()
    checkForRequetsThread = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_balancer_BALANCER_CHECKFOR_REQUEST_THREAD').getValue()

    status = processRequetsThread.isAlive() and checkForRequetsThread.isAlive()

    if status == True:
        pass
    else:
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer.isContainerRunning()]  Return status is: " + str(status))

    return status

def getStatistic(name):

    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler_balancer] Beginning getStatistic()")

    return (getStatistic_common(name))
        
    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler_balancer] Exiting getStatistic()")

def runBalancer():
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Starting Hadoop FS Balancer Task")
    hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()
    #runCommand("sh " + os.path.join(hadoop_bin_dir, "start-balancer.sh"))
    """ Wordaround: version 0.23.7 returns 1 when successful """
    if getHadoopVersion().startswith("0.23"):
        runCommand(getScript("start-balancer.sh"), expectedReturnCodes=[1])
    else:
        runCommand(getScript("start-balancer.sh"))
    
def stopBalancer():
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Stopping existing Balancer if any")
    hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()
    #runCommand("sh " + os.path.join(hadoop_bin_dir, "stop-balancer.sh"))
    """ Wordaround: version 0.23.7 returns 1 when successful """
    if getHadoopVersion().startswith("0.23"):
        runCommand(getScript("stop-balancer.sh"), expectedReturnCodes=[1])
    else:
        runCommand(getScript("stop-balancer.sh"))
    
def retryTimerElapsed():
    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler_balancer] Minimum poll-period timer elapsed.")

def setBalancerReceivedRequests(list):
    stringifiedlist = ""

    for item in list:
        stringifiedlist = stringifiedlist + "," + item 
    stringifiedlist = stringifiedlist[1:]

    proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_balancer_BALANCER_RECEIVED_REQUESTS').setValue(stringifiedlist)

def getBalancerReceivedRequests():  
    stringifiedlist = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_balancer_BALANCER_RECEIVED_REQUESTS').getValue()
    if stringifiedlist == None or stringifiedlist == "":
        requestlist = []
    else:
        requestlist = stringifiedlist.split(',')
    return requestlist

class checkForRequestsThread (threading.Thread):
    def __init__(self, slstdout, slstderr, lock):
        threading.Thread.__init__(self)
        self.slstdout = slstdout
        self.slstderr = slstderr
        self.lock = lock
        
    def run(self):
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Starting checkForRequestsThread.run()")

        """
        Redirect stdout and stderr for this thread. 
        """
        sys.stdout = self.slstdout
        sys.stderr = self.slstderr
 
        try:
            checkForRequests(self.lock)
        except:
            ContainerUtils.getLogger(proxy).severe("[hadoop_enabler_balancer] Unexpected error from checkForRequests thread")
            traceback.print_exc()
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Exiting checkForRequestsThread.run()")

def checkForRequests(lock):

    pollperiod = getContainerRunningConditionPollPeriod()/1000

    while not rcvTrue('hadoop_enabler_balancer_BALANCER_EXIT_FLAG'):

        lock.acquire()
        receivedReqs = getBalancerReceivedRequests()
        currentReqs = getBalancerRequestsFromQueue()
            
        ContainerUtils.getLogger(proxy).fine("[hadoop_enabler_balancer] Current request list [" + str(currentReqs) +  "].")
        ContainerUtils.getLogger(proxy).fine("[hadoop_enabler_balancer] Previous request.list [" + str(receivedReqs) +  "].")
        if receivedReqs != currentReqs: 
            ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Balancer Requests have changed since last checked.")
            proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_balancer_BALANCER_NEW_REQUEST_FLAG').setValue("True")
            setBalancerReceivedRequests(currentReqs)
            stopBalancer()

        lock.release()

        time.sleep(pollperiod)
        
def getBalancerRequestsFromQueue():
    
    hadoop_home_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_HOME_DIR').getValue()
    commandline = os.path.join(hadoop_home_dir, "bin", "hadoop") + " fs -ls " + balancerqueue_dir            
    #Older versions of hadoop (e.g 1.0.4 and 1.1.2) return 255 if file does not exist.  Newer versions return 1
    output = runCommand(commandline, stdout=PIPE, expectedReturnCodes=[0, 255, 1], suppressOutput=True)

    requests = []
    if (output[0] == 0):
        reqsFileList = output[1].splitlines()
        numOfRequests=0
        for i in range(1, len(reqsFileList)):
            if not reqsFileList[i].startswith("Found"):
                """ Workaround - With 2.0.3-alpha a warning message for missing compression libraries is being printed to STDOUT during ls command """
                if not reqsFileList[i].count("Unable to load native-hadoop library for your platform... using builtin-java classes where applicable") > 0: 
                    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler_balancer] Found request in queue [" + str(reqsFileList[i]) + "]")
                    lineTokens = reqsFileList[i].split()
                    if len(lineTokens) == 8:
                        filepath = lineTokens[7]
                        requests.append(filepath)
                        numOfRequests=numOfRequests+1
                    else:
                        ContainerUtils.getLogger(proxy).warning("[hadoop_enabler_balancer] Ignoring Line " + str(i + 1) +  " of queue listing for " + balancerqueue_dir +
                                                             "Line has unexpected format.  Full listing is:" +
                                                             str(reqsFileList) )
        if numOfRequests > 0:
            ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] "+ str(numOfRequests) +" requests found in [" + str(balancerqueue_dir) + "].")
    elif (output[0] == 255) or (output[0] == 1):
        ContainerUtils.getLogger(proxy).fine("[hadoop_enabler_balancer] No requests found in [" + str(balancerqueue_dir) + "].")
    else:
        ContainerUtils.getLogger(proxy).warning("[hadoop_enabler_balancer] Unexpected return code " + str(output[0]) + 
                                                "while trying to get requests from [" + str(balancerqueue_dir) + "].")        
    return requests
        
class processRequestsThread (threading.Thread):
    def __init__(self, slstdout, slstderr, lock):
        threading.Thread.__init__(self)
        self.slstdout = slstdout
        self.slstderr = slstderr
        self.lock = lock
        
    def run(self):
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Starting processRequestsThread.run()")

        """
        Redirect stdout and stderr for this thread. 
        """
        sys.stdout = self.slstdout
        sys.stderr = self.slstderr
 
        try:
            processRequests(self.lock)
        except:
            ContainerUtils.getLogger(proxy).severe("[hadoop_enabler_balancer] Unexpected error from processing thread")
            traceback.print_exc()

        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Exiting processRequestsThread.run()")

def processRequests(lock):
    
    pollperiod = getContainerRunningConditionPollPeriod()/1000
    
    while not rcvTrue('hadoop_enabler_balancer_BALANCER_EXIT_FLAG'):
        
        timer = threading.Timer(pollperiod, retryTimerElapsed)
        timer.start()
            
        lock.acquire()
        if rcvTrue("hadoop_enabler_balancer_BALANCER_NEW_REQUEST_FLAG"):

            ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Beginning processing of new requests")

            receivedReqs = getBalancerReceivedRequests()
            local_receivedReqs = list(receivedReqs)

            proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_balancer_BALANCER_NEW_REQUEST_FLAG').setValue("False")

            lock.release()
            runBalancer()
            lock.acquire()
            
            if not rcvTrue("hadoop_enabler_balancer_BALANCER_NEW_REQUEST_FLAG"):
                ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Deleting processed requests")
                for request in local_receivedReqs:            
                    deleteHDFSFile(request)
                setBalancerReceivedRequests([])
                    
        lock.release()

        if (rcvTrue("hadoop_enabler_balancer_BALANCER_NEW_REQUEST_FLAG") or rcvTrue("hadoop_enabler_balancer_BALANCER_EXIT_FLAG")):
            if timer.isAlive():
                ContainerUtils.getLogger(proxy).info("[hadoop_enabler_balancer] Terminating retry timer early")
                timer.cancel()
        else:
            timer.join()
