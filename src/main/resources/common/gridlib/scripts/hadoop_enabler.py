from com.datasynapse.fabric.admin.info import AllocationInfo
from com.datasynapse.fabric.util import GridlibUtils, ContainerUtils
from com.datasynapse.fabric.common import RuntimeContextVariable, ActivationInfo
from subprocess import Popen, PIPE, STDOUT

import os
import time
import socket
import getpass
import httplib
import sys
import threading

sys.path.append(proxy.getContainer().getScript(0).getFile().getParentFile().getAbsolutePath())
from hadoop_enabler_common import *
ContainerUtils.getLogger(proxy).info("sys.path modified: " + str(sys.path) )

host = socket.gethostname()
hostip = socket.gethostbyname(socket.gethostname())
decommissionqueue_dir = "/.fabric/decom-queue/"
recommissionqueue_dir = "/.fabric/recom-queue/"

def doInit(additionalVariables):
    
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Beginning doInit()")

    doInit_common(additionalVariables)
    
    if rcvTrue("hadoop_enabler_ENABLE_NAMENODE"):
        doInit_namenode(additionalVariables)
        
    if rcvTrue("hadoop_enabler_ENABLE_SECONDARYNAMENODE"):
        additionalVariables.add(RuntimeContextVariable("hadoop_enabler_SECONDARYNAMENODE_PID", "0", 
                                                   RuntimeContextVariable.STRING_TYPE, "Tasktracker's PID", False, RuntimeContextVariable.NO_INCREMENT))

    if rcvTrue("hadoop_enabler_ENABLE_DATANODE"):
        additionalVariables.add(RuntimeContextVariable("hadoop_enabler_DATANODE_PID", "0", 
                                                   RuntimeContextVariable.STRING_TYPE, "Tasktracker's PID", False, RuntimeContextVariable.NO_INCREMENT))

    if rcvTrue("hadoop_enabler_ENABLE_JOBTRACKER"):
        additionalVariables.add(RuntimeContextVariable("hadoop_enabler_JOBTRACKER_HOST", host, 
                                                   RuntimeContextVariable.STRING_TYPE, "Job Tracker Host Name", True, RuntimeContextVariable.NO_INCREMENT))

        additionalVariables.add(RuntimeContextVariable("hadoop_enabler_JOBTRACKER_HOST_IP", hostip, 
                                                   RuntimeContextVariable.STRING_TYPE, "Jobtracker's IP Address", True, RuntimeContextVariable.NO_INCREMENT))
        
        additionalVariables.add(RuntimeContextVariable("hadoop_enabler_JOBTRACKER_PID", "0", 
                                                   RuntimeContextVariable.STRING_TYPE, "Jobtracker's PID", False, RuntimeContextVariable.NO_INCREMENT))

    if rcvTrue("hadoop_enabler_ENABLE_TASKTRACKER"):
        additionalVariables.add(RuntimeContextVariable("hadoop_enabler_TASKTRACKER_PID", "0", 
                                                   RuntimeContextVariable.STRING_TYPE, "Tasktracker's PID", False, RuntimeContextVariable.NO_INCREMENT))

    proxy.doInit(additionalVariables)
    
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Exiting doInit()")

def doStart():
    
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Beginning doStart()")

    doStart_common()                
    
    if rcvTrue('hadoop_enabler_ENABLE_NAMENODE') and \
       rcvTrue('hadoop_enabler_ENABLE_SECONDARYNAMENODE') and \
       rcvTrue('hadoop_enabler_ENABLE_DATANODE') and \
       rcvTrue('hadoop_enabler_ENABLE_JOBTRACKER') and \
       rcvTrue('hadoop_enabler_ENABLE_TASKTRACKER'):

            hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()
            
            excludefilename = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HDFS_EXCLUDE_FILE').getValue()
            runCommand("touch " + excludefilename)
            
            formatNamenode()

            formatDatanode()

            ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Start all Hadoop Daemons")
            if ContainerUtils.isWindows():
                raise Exception("[hadoop_enabler] Windows not yet implemented in by this enabler.")
            else:
                 commandline = "sh "+ os.path.join(hadoop_bin_dir, "start-all.sh")        
            runCommand(commandline)

            proxy.getContainer().getRuntimeContext().getVariable("hadoop_enabler_NAMENODE_PID").setValue(capturePID("proc_namenode"))
            proxy.getContainer().getRuntimeContext().getVariable("hadoop_enabler_SECONDARYNAMENODE_PID").setValue(capturePID("proc_secondarynamenode"))
            proxy.getContainer().getRuntimeContext().getVariable("hadoop_enabler_DATANODE_PID").setValue(capturePID("proc_datanode"))
            proxy.getContainer().getRuntimeContext().getVariable("hadoop_enabler_JOBTRACKER_PID").setValue(capturePID("proc_jobtracker"))
            proxy.getContainer().getRuntimeContext().getVariable("hadoop_enabler_TASKTRACKER_PID").setValue(capturePID("proc_tasktracker"))
            
            runCommand(os.path.join(hadoop_bin_dir, "hadoop") + " fs -mkdir " + decommissionqueue_dir, expectedReturnCodes=[0, 255])
            runCommand(os.path.join(hadoop_bin_dir, "hadoop") + " fs -mkdir " + recommissionqueue_dir, expectedReturnCodes=[0, 255])        

            processDatanodeRequestsThread = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_NAMENODE_PROCESS_DATANODE_REQUESTS_THREAD').getValue()
            processDatanodeRequestsThread.start()

    else:
        if rcvTrue('hadoop_enabler_ENABLE_NAMENODE'):
            
            hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()

            excludefilename = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HDFS_EXCLUDE_FILE').getValue()
            runCommand("touch " + excludefilename)
            
            formatNamenode()

            ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Starting Hadoop Namenode Daemon")
            startDaemon("namenode", 'hadoop_enabler_NAMENODE_PID')                    

            runCommand(os.path.join(hadoop_bin_dir, "hadoop") + " fs -mkdir " + decommissionqueue_dir)        
            runCommand(os.path.join(hadoop_bin_dir, "hadoop") + " fs -mkdir " + recommissionqueue_dir)        

            processDatanodeRequestsThread = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_NAMENODE_PROCESS_DATANODE_REQUESTS_THREAD').getValue()
            processDatanodeRequestsThread.start()
            
        if rcvTrue('hadoop_enabler_ENABLE_SECONDARYNAMENODE'):
            
            """ Hadoop 2.03-alpha requires the exclude file be present on the secondary namenode """
            excludefilename = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HDFS_EXCLUDE_FILE').getValue()
            runCommand("touch " + excludefilename)

            ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Starting Hadoop Secondary Namenode Daemon")
            startDaemon("secondarynamenode", 'hadoop_enabler_SECONDARYNAMENODE_PID')                    
        
        if rcvTrue('hadoop_enabler_ENABLE_DATANODE'):           
            formatDatanode()
            doStart_datanode() 

        if rcvTrue('hadoop_enabler_ENABLE_JOBTRACKER'):
            ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Starting Hadoop Jobtracker Daemon")
            startDaemon("jobtracker", 'hadoop_enabler_JOBTRACKER_PID')                    
        
        if rcvTrue('hadoop_enabler_ENABLE_TASKTRACKER'):
            ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Starting Hadoop Tasktracker Daemon")
            startDaemon("tasktracker", 'hadoop_enabler_TASKTRACKER_PID')                    
                    
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Exiting doStart()")

def doShutdown():
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Beginning doShutdown()")
    hadoop_bin_dir_var = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR')
    if hadoop_bin_dir_var != None:
        hadoop_bin_dir = hadoop_bin_dir_var.getValue()        

        if rcvTrue('hadoop_enabler_ENABLE_NAMENODE') and \
           rcvTrue('hadoop_enabler_ENABLE_SECONDARYNAMENODE') and \
           rcvTrue('hadoop_enabler_ENABLE_DATANODE') and \
           rcvTrue('hadoop_enabler_ENABLE_JOBTRACKER') and \
           rcvTrue('hadoop_enabler_ENABLE_TASKTRACKER'):
    
                ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Stopping all Hadoop Daemons")
                
                if ContainerUtils.isWindows():
                    raise Exception("[hadoop_enabler] Windows not yet implemented in by this enabler.")
                else:
                     commandline = "sh "+ os.path.join(hadoop_bin_dir, "stop-all.sh")        
                runCommand(commandline)
                                
        else:
            if rcvTrue('hadoop_enabler_ENABLE_NAMENODE'):
                ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Stopping Hadoop Namenode Daemon")
                
                proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_ENABLER_SHUTTING_DOWN').setValue("True")
                stopDaemon("namenode")
                
                processDatanodeRequestsThread = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_NAMENODE_PROCESS_DATANODE_REQUESTS_THREAD').getValue()
                if processDatanodeRequestsThread.isAlive():
                    processDatanodeRequestsThread.join()
                
                
                
            if rcvTrue('hadoop_enabler_ENABLE_SECONDARYNAMENODE'):
                ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Stopping Hadoop Secondary Namenode Daemon")
                stopDaemon("secondarynamenode")                    
            
            if rcvTrue('hadoop_enabler_ENABLE_DATANODE'):
                doShutDown_datanode()
             
            if rcvTrue('hadoop_enabler_ENABLE_JOBTRACKER'):
                ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Stopping Hadoop Jobtracker Daemon")
                stopDaemon("jobtracker")                    
            
            if rcvTrue('hadoop_enabler_ENABLE_TASKTRACKER'):
                ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Stopping Hadoop Tasktracker Daemon")
                stopDaemon("tasktracker")                    
            
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Exiting doShutdown()")
    
def getCurComponentName():
    return proxy.getContainer().getCurrentDomain().getName()

def startDaemon(daemonname, pidvariable):
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Starting Hadoop Daemon: [" + daemonname + "]")
    hadoopDaemonScript = getScript("hadoop-daemon.sh")            
    hadoop_conf_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_CONF_DIR').getValue()
    runCommand(hadoopDaemonScript + " --config " + hadoop_conf_dir + " start " + daemonname)

    #runCommand(os.path.join(hadoop_bin_dir, "hadoop") + " " + daemonname)
    
    pid = capturePID(daemonname)
    proxy.getContainer().getRuntimeContext().getVariable(pidvariable).setValue(pid)
    
def capturePID(identifyingString):     
    hadoop_home_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_HOME_DIR').getValue()

    if ContainerUtils.isWindows():
        raise Exception("[hadoop_enabler] Windows not yet implemented in by this enabler.")
    else:
        commandline = "ps x"
    output = runCommand(commandline, expectedReturnCodes=[0], stdout=PIPE, suppressOutput=True)

    matchesFound = 0
    for line in output[1].splitlines():
        if hadoop_home_dir in line:
            if identifyingString in line:
                pid = int(line.split(None, 1)[0])
                ContainerUtils.getLogger(proxy).info("[hadoop_enabler] PID for [" + str(identifyingString) + "] is [" + str(pid) + "]")
                matchesFound = matchesFound + 1
        
    if (matchesFound == 1):
        return str(pid)
    else:
        ContainerUtils.getLogger(proxy).severe("[hadoop_enabler] [" + str(matchesFound) + "] processes found that match filters [" +
                                                   str(hadoop_home_dir) + "] and [" + str(identifyingString) + "]. Expected 1 match.")
        ContainerUtils.getLogger(proxy).severe("Full 'ps x' output is: " + str(output[1]))
        raise Exception("[hadoop_enabler] Unable to identify PID for started daemon that matches identifying string [" + str(identifyingString) + "].")
        return 0

def stopDaemon(daemonname):
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Stopping Hadoop Daemon: [" + daemonname + "]")
    hadoopDaemonScript = getScript("hadoop-daemon.sh")        
    hadoop_conf_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_CONF_DIR').getValue()        
    runCommand(hadoopDaemonScript + " --config " + hadoop_conf_dir + " stop " + daemonname)

def hasContainerStarted():
    status = isContainerRunning_silent()    
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler.hasContainerStarted()]  Return status is: " + str(status))
    return status

def isContainerRunning():
    
    status = isContainerRunning_silent()
               
    if status == True:
        pass
    else:
        ContainerUtils.getLogger(proxy).warning("[hadoop_enabler.isContainerRunning()]  Return status is: " + str(status))
        
    return status
    
def isContainerRunning_silent():
    
    status  =  isPIDRunning("NameNode", "hadoop_enabler_ENABLE_NAMENODE" , "hadoop_enabler_NAMENODE_PID") and \
               isPIDRunning("Secondary NameNode", "hadoop_enabler_ENABLE_SECONDARYNAMENODE" , "hadoop_enabler_SECONDARYNAMENODE_PID") and \
               isPIDRunning("DataNode", "hadoop_enabler_ENABLE_DATANODE" , "hadoop_enabler_DATANODE_PID") and \
               isPIDRunning("Job Track", "hadoop_enabler_ENABLE_JOBTRACKER" , "hadoop_enabler_JOBTRACKER_PID") and \
               isPIDRunning("Task Tracker", "hadoop_enabler_ENABLE_TASKTRACKER" , "hadoop_enabler_TASKTRACKER_PID")
    """
    status  =  isWebUIRunning("NameNode", "hadoop_enabler_ENABLE_NAMENODE" , "hadoop_enabler_NAMENODE_HTTP_BASEPORT") and \
               isWebUIRunning("Secondary NameNode", "hadoop_enabler_ENABLE_SECONDARYNAMENODE" , "hadoop_enabler_SECONDARYNAMENODE_HTTP_BASEPORT") and \
               isWebUIRunning("DataNode", "hadoop_enabler_ENABLE_DATANODE" , "hadoop_enabler_DATANODE_HTTP_BASEPORT") and \
               isWebUIRunning("Job Track", "hadoop_enabler_ENABLE_JOBTRACKER" , "hadoop_enabler_JOBTRACKER_HTTP_BASEPORT") and \
               isWebUIRunning("Task Tracker", "hadoop_enabler_ENABLE_TASKTRACKER" , "hadoop_enabler_TASKTRACKER_HTTP_BASEPORT")
    """
    if rcvTrue('hadoop_enabler_ENABLE_NAMENODE'):            
        processDatanodeRequestsThread = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_NAMENODE_PROCESS_DATANODE_REQUESTS_THREAD').getValue()
        
        threadrunning = processDatanodeRequestsThread.isAlive()
        status = status and threadrunning
        if not threadrunning:
            ContainerUtils.getLogger(proxy).severe("[hadoop_enabler] Processing thread for managing datanode requests has died unexpectedly" )
                         
    return status

def isPIDRunning(description, enabledFlag,  pidvariable):
    if rcvTrue(enabledFlag): 

        status = False 
        
        pid = proxy.getContainer().getRuntimeContext().getVariable(pidvariable).getValue()
        
        if ContainerUtils.isWindows():
            raise Exception("[hadoop_enabler] Windows not yet implemented in by this enabler.")
        else:
            commandline = "ps x"
        output = runCommand(commandline, expectedReturnCodes=[0], stdout=PIPE, suppressOutput=True)
    
        for line in output[1].splitlines():
            if str(pid) == line.split(None, 1)[0]:
                status = True
                break
        if status != True:
            ContainerUtils.getLogger(proxy).severe("[hadoop_enabler] " + str(description) + " process has failed.  PID [" + str(pid) +  "] not found." )
    else:
        status = True
        
    return status
    
def isWebUIRunning(uiDesc, enabledFlag,  uiPort_rcv):

    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Testing Web UI " + uiDesc + " on port [" + uiPort_rcv + "]" )
    if rcvTrue(enabledFlag): 

        retryperiod = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_ENABLER_RUNNING_RETRYPERIOD').getValue()
        retrymax = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_ENABLER_RUNNING_RETRYMAX').getValue()

        success = testWebUI(uiDesc, uiPort_rcv)
        i = 0
        while ((i < int(retrymax)) and (success == False)):
            ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] Retrying test in [" + str(retryperiod) + "] seconds" )
            time.sleep(float(retryperiod))
            success = testWebUI(uiDesc, uiPort_rcv) 
            i = i + 1 
            
        if (success == False):
            ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] Max retries [" + str(retrymax) + "] exhausted.  Test failed." )
        return success
    else:
        return True
    
def testWebUI(uiDesc, uiPort_rcv):
    port = proxy.getContainer().getRuntimeContext().getVariable(uiPort_rcv).getValue()
    
    url = str(hostip) + ":" + str(port)
    try:
        conn = httplib.HTTPConnection(url)
        conn.request("HEAD", "/")
        r1 = conn.getresponse()
        if (r1.status == 200) and (r1.reason == "OK"):
            ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] HTTP connection attempt to " + uiDesc + " at [" + url + "] successful.  Status: [" + str(r1.status) + "].  Reason: [" + r1.reason +"]")
            return True
        else:  
            ContainerUtils.getLogger(proxy).info("[hadoop_enabler] HTTP connection attempt to " + uiDesc + " at [" + url + "] successful.  Status: [" + str(r1.status) + "].  Reason: [" + r1.reason +"]")
            return False
    except socket.error:
        ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] HTTP connection attempt to " + uiDesc + " at [" + url + "] caused socket error.")
        return False

def doInstall(info):
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Beginning doInstall()")

    routenumber = 0
    if rcvTrue('hadoop_enabler_ENABLE_NAMENODE'):
        setStaticRoute(info, str(routenumber), "/nn", proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_NAMENODE_HTTP_BASEPORT').getValue())
        routenumber = routenumber + 1
    if rcvTrue('hadoop_enabler_ENABLE_SECONDARYNAMENODE'):
        setStaticRoute(info, str(routenumber), "/sn", proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_SECONDARYNAMENODE_HTTP_BASEPORT').getValue())
        routenumber = routenumber + 1
    if rcvTrue('hadoop_enabler_ENABLE_DATANODE'):
        setStaticRoute(info, str(routenumber), "/dn", proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_DATANODE_HTTP_BASEPORT').getValue())
        routenumber = routenumber + 1
    if rcvTrue('hadoop_enabler_ENABLE_JOBTRACKER'):
        setStaticRoute(info, str(routenumber), "/jt", proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_JOBTRACKER_HTTP_BASEPORT').getValue())
        routenumber = routenumber + 1
    if rcvTrue('hadoop_enabler_ENABLE_TASKTRACKER'):
        setStaticRoute(info, str(routenumber), "/tt", proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_TASKTRACKER_HTTP_BASEPORT').getValue())

    info.setProperty(ActivationInfo.REDIRECT_TO_ENDPOINTS,"true")

    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Exiting doInstall()")    
    
def setStaticRoute(info, routenum, prefix, port):
    
    #: expand to support https
    
    httpinfo = features.get("HTTP Support")

    userprefix = ""
    if httpinfo:
        routingprefix = httpinfo.getRoutingPrefix()
        if ((routingprefix != None) and (str(routingprefix) != "")):
            userprefix = "/" + routingprefix
    
    fullprefix = str(userprefix) + prefix
    ContainerUtils.getLogger(proxy).info("Setting http routing prefix [" + fullprefix + "]")
                
    info.setProperty(ActivationInfo.HTTP_STATIC_ROUTE_PREFIX + routenum, fullprefix + ";http://"+ host + ":" + port+ "/");    

def getStatistic(name):

    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Beginning getStatistic()")

    if rcvTrue('hadoop_enabler_ENABLE_NAMENODE'):
    
        if name.startswith('enabler_NAMEDIR_'):
            namenodeDir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_NAMENODE_NAME_DIR').getValue()
            namenodeDirList = namenodeDir.split(",")
            
            if name == "enabler_NAMEDIR_FREE":
                free = []
                if namenodeDirList != None and len(namenodeDirList) > 0:
                    for directory in namenodeDirList:
                        directoryStripped = directory.lstrip().rstrip()
                        if not (directoryStripped == ""):
                            free.append(getStatistic_disk(directoryStripped)[0])
                else:
                    ContainerUtils.getLogger(proxy).severe("[hadoop_enabler] No directories found in ${hadoop_enabler_NAMENODE_NAME_DIR}")  #TODO change to use defulat value
                if free != None and len(free) > 0:
                    return min(free)
                else:
                    return 0
            elif name == "enabler_NAMEDIR_USED_PERCENT":
                usedPercent = []
                if namenodeDirList != None and len(namenodeDirList) > 0:
                    for directory in namenodeDirList:
                        directoryStripped = directory.lstrip().rstrip()
                        if not (directoryStripped == ""):
                            usedPercent.append(getStatistic_disk(directoryStripped)[2])
                else:
                    ContainerUtils.getLogger(proxy).severe("[hadoop_enabler] No directories found in ${hadoop_enabler_NAMENODE_NAME_DIR}")  #TODO change to use defulat value
                if usedPercent != None and len(usedPercent) > 0:
                    return max(usedPercent)
                else:
                    return 0            
            else:
                raise Exception("[hadoop_enabler] Unsupported statistic type requested [" + str(name) + "]")

        else:
            return (getStatistic_common(name))        
    else:
        return (getStatistic_common(name))        
    
    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Exiting getStatistic()")

""" **********************************************************
Namenode Functions
********************************************************** """

def doInit_namenode(additionalVariables):

    additionalVariables.add(RuntimeContextVariable("hadoop_enabler_NAMENODE_HOST", host, 
                                               RuntimeContextVariable.STRING_TYPE, "Namenode's Host Name", True, RuntimeContextVariable.NO_INCREMENT))
    additionalVariables.add(RuntimeContextVariable("hadoop_enabler_NAMENODE_HOST_IP", hostip, 
                                               RuntimeContextVariable.STRING_TYPE, "Namenode's IP Address", True, RuntimeContextVariable.NO_INCREMENT)) #TODO Do not export after distributed NN 
    additionalVariables.add(RuntimeContextVariable("hadoop_enabler_NAMENODE_COMPONENT", getCurComponentName(), 
                                               RuntimeContextVariable.STRING_TYPE, "Namenode's Component Name", True, RuntimeContextVariable.NO_INCREMENT))
    additionalVariables.add(RuntimeContextVariable("hadoop_enabler_NAMENODE_PID", "0", 
                                               RuntimeContextVariable.STRING_TYPE, "Namenode's PID", False, RuntimeContextVariable.NO_INCREMENT))
            
    """ Create New Thread object for processing requests and save it for later. """
    logger = ContainerUtils.getLogger(proxy)
    slstdout = StreamToLogger(logger,"STDOUT")
    slstderr = StreamToLogger(logger,"STDERR")
    processDatanodeRequestsThread = ProcessDatanodeRequestsThread(slstdout, slstderr)
    additionalVariables.add(RuntimeContextVariable("hadoop_enabler_NAMENODE_PROCESS_DATANODE_REQUESTS_THREAD", processDatanodeRequestsThread, 
                                                   RuntimeContextVariable.OBJECT_TYPE))
    
    """ Shutdown switch """
    additionalVariables.add(RuntimeContextVariable("hadoop_enabler_ENABLER_SHUTTING_DOWN", "False",
                                                   RuntimeContextVariable.STRING_TYPE))

def formatNamenode(): 
    
    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Beginning formatNamenode()")
    
    hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()
    workdir = proxy.getContainer().getRuntimeContext().getVariable('CONTAINER_WORK_DIR').getValue()
    namenodeDir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_NAMENODE_NAME_DIR').getValue()
    
    namenodeDirList = namenodeDir.split(",")
    
    preexistingNameDirs = []
    newNameDirs = []
    persistentNameDir = False
    for directory in namenodeDirList:
        directoryStripped = directory.lstrip().rstrip()
        if not (directoryStripped == ""):
            ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Namenode Name Directory path defined [" + str(directoryStripped) + "]")
            if (os.path.isdir(directoryStripped) == True):
                ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Directory already exists")
                preexistingNameDirs.append(directoryStripped)
            else:
                newNameDirs.append(directoryStripped)

            if not directoryStripped.startswith(workdir):
                persistentNameDir = True

    if not persistentNameDir:
        ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] No persistent location has been defined for the name directory.  Namenode metadata will be lost when Namenode stops or restarts.")

    if (preexistingNameDirs == None) or (len(preexistingNameDirs) > 0):
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Will bypass formatting since one or more of the defined Namenode Name Directories already exists.]")
        if not newNameDirs == None:
            for directory in newNameDirs:
                ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Creating new Namenode Name Directory [" + str(directory) + "]")
                os.makedirs(directory)
                if ContainerUtils.isWindows():
                    raise Exception("[hadoop_enabler] Windows not yet implemented in by this enabler.")
                else:
                    os.utime(directory, (0,0))        
    else:
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Formatting distributed-filesystem")
        commandline = os.path.join(hadoop_bin_dir, "hadoop") + " namenode -format"
        runCommand(commandline)    
        
    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Exiting formatNamenode()")

def formatDatanode(): 
    
    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Beginning formatDatanode()")
    
    workdir = proxy.getContainer().getRuntimeContext().getVariable('CONTAINER_WORK_DIR').getValue()
    datanodeDir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_DATANODE_DATA_DIR').getValue()
    if datanodeDir == None or datanodeDir == "":
        ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] No Datanode Directory location defined.  All datablocks will be lost if HDFS cluster is stopped or datanode fails unexpectedly.")    
    else:
        datanodeDirList = datanodeDir.split(",")
        for directory in datanodeDirList:
            directoryStripped = directory.lstrip().rstrip()
            if not (directoryStripped == ""):
                ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Datanode Data Directory path defined [" + str(directoryStripped) + "]")
                if (os.path.isdir(directoryStripped) == True):
                    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Directory already exists")
                else:
                    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Creating new Datanode Data Directory [" + str(directoryStripped) + "]")
                    os.makedirs(directoryStripped)
                    
    
                if directoryStripped.startswith(workdir):
                    ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] Datanode Directory [" + str(directoryStripped) + "] is in Enablers temporary directoy.  .")    
        
    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Exiting formatDatanode()")

def nameDirectoryPersistenceWarning ():

        """ Name Directory(s) list validation """
        persistentNameDir = False

        namenodeDir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_NAMENODE_NAME_DIR').getValue()
        workdir = proxy.getContainer().getRuntimeContext().getVariable('CONTAINER_WORK_DIR').getValue()
        namenodeDirList = namenodeDir.split(",")
        
        for directory in namenodeDirList:
            if not (directory == ""):
                if not directory.startswith(workdir):
                    persistentNameDir = True

        if not persistentNameDir:
            ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] No persistent location has been defined for the name directory.  Namenode metadata will be lost when Namenode stops or restarts.")
                
"""
def formatNamenode(): 

    hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()

    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Beginning formatNamenode()")
    
    localnamedir = proxy.getContainer().getRuntimeContext().getVariable(localnamedir_varname).getValue()
    remotenamedir = proxy.getContainer().getRuntimeContext().getVariable(remotenamedir_varname).getValue()

    ContainerUtils.getLogger(proxy).fine("Testing local and remote name directories to determine formatting plan")
    
    if (localnamedir == "") and (remotenamedir == ""):
        ContainerUtils.getLogger(proxy).fine("local !defined, remote !defined")
        raise Exception("[hadoop_enabler] No name directory defined.  Must set Silver Fabric RuntimeContext variable ${" + str(localnamedir_varname) + "} or ${" + str(remotenamedir_varname) + "} ")
    elif (localnamedir != "") and (remotenamedir != ""):
        ContainerUtils.getLogger(proxy).fine("local defined remote defined")
        if (os.path.isdir(localnamedir) == True) and (os.path.isdir(remotenamedir) == True):
            ContainerUtils.getLogger(proxy).fine("local exists, remote exists")
            format = False
        if (os.path.isdir(localnamedir) == False) and (os.path.isdir(remotenamedir) == False):
            ContainerUtils.getLogger(proxy).fine("local !exists, remote !exists")
            format = True
        if (os.path.isdir(localnamedir) == False) and (os.path.isdir(remotenamedir) == True):
            ContainerUtils.getLogger(proxy).fine("local !exists, remote exists")
            os.makedirs(localnamedir)
            time.sleep(0.002) # Sleep two milliseconds so we can be 100% sure the remote directory will have a more recent time stamp than the
                              # local directory we just created above.
            os.utime(remotenamedir, None)
            format = False
        if (os.path.isdir(localnamedir) == True) and (os.path.isdir(remotenamedir) == False):
            ContainerUtils.getLogger(proxy).fine("local exists, remote !exists")
            os.makedirs(remotenamedir)
            time.sleep(0.002) # Sleep two milliseconds so we can be 100% sure the remote directory will have a more recent time stamp than the
                              # local directory we just created above.
            os.utime(localnamedir, None)
            format = False
    elif (localnamedir == "") and (remotenamedir != ""):
        ContainerUtils.getLogger(proxy).fine("local !defined, remote defined")
        if (os.path.isdir(remotenamedir) == True):
            ContainerUtils.getLogger(proxy).fine("remote exists")
            format = False
        else:
            ContainerUtils.getLogger(proxy).fine("remote !exists")
            format = True
    elif (localnamedir != "") and (remotenamedir == ""):
        ContainerUtils.getLogger(proxy).fine("local defined, remote !defined")
        if (os.path.isdir(localnamedir) == True):
            ContainerUtils.getLogger(proxy).fine("local exists")
            format = False
        else:
            ContainerUtils.getLogger(proxy).fine("local !exists")
            format = True
        
    if (format):
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Formatting distributed-filesystem")
        commandline = os.path.join(hadoop_bin_dir, "hadoop") + " namenode -format"
        runCommand(commandline)    
    else:
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Bypassing the formatting of the distributed-filesystem")

    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Exiting formatNamenode()")
"""
class ProcessDatanodeRequestsThread (threading.Thread):
    def __init__(self, slstdout, slstderr):
        threading.Thread.__init__(self)
        self.slstdout = slstdout
        self.slstderr = slstderr
        
    def run(self):
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Starting processDataNodesThread.run()")

        """
        Redirect stdout and stderr for this thread. 
        """
        sys.stdout = self.slstdout
        sys.stderr = self.slstderr
 
        try:
            processDatanodeRequests()
        except:
            ContainerUtils.getLogger(proxy).severe("[hadoop_enabler] Unexpected error from processing thread")
            traceback.print_exc()

        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Exiting processDataNodesThread.run()")

def processDatanodeRequests():
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Beginning processDatanodeRequests()")
    
    hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()
    excludefilename = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HDFS_EXCLUDE_FILE').getValue()
    
    pollperiod = getContainerRunningConditionPollPeriod()/1000
    
    excludefileempty = False #Assume exclude file is not empty at startup in case this is a restart
    
    while not rcvTrue('hadoop_enabler_ENABLER_SHUTTING_DOWN'):
        
        """
        Receive Requests
        """
        validDecomReqs = []
        
        ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Getting queued Decommission Requests")
        decomReqs = getRequestsFromQueue(decommissionqueue_dir)
        #if (len(decomReqs) > 0 ):
        #
        #    for req in decomReqs:
        #        datanodehost = req[0]
        #        validDecomReqs.append(datanodehost)

        validDecomReqs = decomReqs

        ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Getting queued Recommission Requests")
        recomReqs = getRequestsFromQueue(recommissionqueue_dir)
         
        if (len(recomReqs) > 0 ):
        
            for req in recomReqs:
                datanodehost = req[0]
                if (validDecomReqs.count(datanodehost) > 0):
                    validDecomReqs.remove(datanodehost)
                    
                else:
                    ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] Recommission request [" + str(datanodehost) + 
                                                         "] recieved but unable to find matching decommissioning request to delete.  Continuing with processing.")
        
        """
        Handle fully decommissioned datanodes in request list 
        """
        if (len(decomReqs) > 0 ):
            """  Get list of fully decommissioned hosts """
            decommissionedHosts = getDecommssionedHosts()
        
            """
            Kill any Fully Decommisioned datanodes
            
            Note: This is required starting with version 0.20 of Hadoop.  With earlier versions 
                  the Datanode will terminates automatically once decomissioning is complete.
            """
            #for request in decomReqs:
            for request in validDecomReqs:
                if (decommissionedHosts.count(request[0]) > 0):
                    if ContainerUtils.isWindows():
                        raise Exception("[hadoop_enabler] Windows not yet implemented in by this enabler.")
                    else:
                        commandline = os.path.join(hadoop_bin_dir, "hadoop") + " fs -cat " + request[1]            
                    output = runCommand(commandline, stdout=PIPE, expectedReturnCodes=[0])
            
                    if (output[0] == 0):
                        #pid = output[1]
                        """ Workaround to handle warning statment being placed in STDOUT.  Needed for some Hadoop versions (e.g. 0.23.7). """
                        pid = output[1].splitlines()[-1]

                        host = (request[0].split(":"))[0]
                        killDatanode(host, pid)
                    else:
                        ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] Unexpected return code " + output[0] + "while trying to read decommission requests."  + filename + 
                                                                "]. Bypassing request.")
            """
            Remove Fully Deommissioned Hosts from exclude list 
            """
            if (len(decommissionedHosts) > 0 ):
            
                for datanodehost in decommissionedHosts:
                    if (validDecomReqs.count(datanodehost) > 0):
                        validDecomReqs.remove(datanodehost)
        else:
            """ if there are no current decommission Requests, then we will have no need to process the list of hosts that have completed decommissioning """
            decommissionedHosts = []
        
        """
        Update the Namenode
        """
        if not(len(validDecomReqs) == 0 and excludefileempty): 

            excludefilename = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HDFS_EXCLUDE_FILE').getValue()
        
            excludefile = open(excludefilename, 'w')
            for request in validDecomReqs:
                excludefile.write("%s\n" % request[0])  
            excludefile.close()
                                                        
            refreshNodes()
        
        if len(validDecomReqs) == 0:
            excludefileempty = True

        """
        Clean up Completed Request Queues
        """

        """ Delete Completed Decom Requests """
        for request in decomReqs:
            if (decommissionedHosts.count(request[0]) > 0):
                deleteHDFSFile(request[1])

        """ Delete Decom Requests for which there is a mactchin recom request"""
        for request in recomReqs:            
            decomrequest = os.path.join(decommissionqueue_dir, str(request[0]).replace(":", "-", 1))
            deleteHDFSFile(decomrequest) 
            
            deleteHDFSFile(request[1])
            
        ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Waiting for requests.  Will check again in [" + str(pollperiod) + "] seconds")
        
        """
        Pause before repeating
        """
        time.sleep(pollperiod)    
        
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Exiting processDatanodeRequests()") 

def killDatanode(host, pid):     
    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Beginning killDatanode()")
    
    if ContainerUtils.isWindows():
        raise Exception("[hadoop_enabler] Windows not yet implemented by this enabler.")
    else:
        commandline = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o PasswordAuthentication=no " + host + " kill -9 " + str(pid)            
    output = runCommand(commandline, expectedReturnCodes=[0, 1])
    
    if (output[0] == 0):
        pass
    elif (output[0] == 1):
        ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] Datanode process [" + str(pid) + "] was already terminated on host [" + 
                                                host +"].  This is expected behavior prior to version 0.20 of Hadoop")
    else:
        ContainerUtils.getLogger(proxy).severe("[hadoop_enabler] Unexpected return code while trying to kill Datanode process [" + str(pid) + 
                                                "] on host [" + host +"].") 

    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Exiting killDatanode()")

def getRequestsFromQueue(queue_dir):
    
    hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()
    if ContainerUtils.isWindows():
        raise Exception("[hadoop_enabler] Windows not yet implemented in by this enabler.")
    else:
            commandline = "sh " + os.path.join(hadoop_bin_dir, "hadoop") + " fs -ls " + queue_dir            
    output = runCommand(commandline, stdout=PIPE, suppressOutput=True)
    #output = runCommand(commandline, stdout=PIPE, expectedReturnCodes=[0, 255], suppressOutput=True)

    requests = []
    if (output[0] == 0):
        ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Requests are: [" + str(output[1]) + "].")
        reqsFileList = output[1].splitlines()
        numOfRequests=0
        for i in range(1, len(reqsFileList)):
            if not reqsFileList[i].startswith("Found"):
                """ Workaround - With 2.0.3-alpha a warning message for missing compression libraries is being printed to STDOUT during ls command """
                if not reqsFileList[i].count("Unable to load native-hadoop library for your platform... using builtin-java classes where applicable") > 0: 
                    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Processing request [" + str(reqsFileList[i]) + "]")
                    lineTokens = reqsFileList[i].split()
                    if len(lineTokens) == 8:
                        filepath = lineTokens[7]
                        filebasename = os.path.basename(filepath)            
                        requests.append([filebasename.replace("-", ":", 1), filepath])
                        numOfRequests=numOfRequests+1         
                    else:
                        ContainerUtils.getLogger(proxy).warning("[hadoop_enabler_balancer] Ignoring Line " + str(i + 1) + " of queue listing for " + queue_dir + 
                                                             ". Line has unexpected format.  Full listing is:" +
                                                             str(reqsFileList) )                         
        if numOfRequests > 0:
            ContainerUtils.getLogger(proxy).info("[hadoop_enabler] "+ str(numOfRequests) +" requests found in [" + str(queue_dir) + "].")
    elif (output[0] == 255):
        ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] No requests found in [" + str(queue_dir) + "].")
    else:
        ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] Unexpected return code " + str(output[0]) + 
                                                " while trying to get requests from [" + str(queue_dir) + "].")        
    return requests
        
def getDecommssionedHosts():

    hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()
    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Beginning getDecommssionedHosts()")
    
    decommissionedHostList = []
    prevName = None
    lineNumber = 0

    os.unsetenv("LD_LIBRARY_PATH")
    os.unsetenv("LD_PRELOAD")

    args = shlex.split(os.path.join(hadoop_bin_dir, "hadoop") + " dfsadmin -report")
    proc=Popen(args, stdout=PIPE, stderr=STDOUT)
    for line in proc.stdout:
        lineNumber = lineNumber + 1
        if line.startswith("Name:"):
            tokens = line.split()
            #if (len(tokens) == 2): 
            prevName = tokens[1]
            #else:
            #    ContainerUtils.getLogger(proxy).severe("[hadoop_enabler] Error parsing dfsadmin report line number [" + 
            #                                           str(lineNumber) + "].  Expect 2 tokens on lines begining with [" + "Name :" + 
            #                                           "]. But line contains [" + str(len(tokens)) + "] tokens.  Line content is [" + line + "]")
            #    prevName = None
        elif line.startswith("Decommission Status : "):
            status =  line[len("Decommission Status : "):]
            ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Decommission status for [" + str(prevName) + "]  is [" + str(status) + "].")
            if (status.startswith("Decommissioned")):
                if (prevName == None):
                    ContainerUtils.getLogger(proxy).severe("[hadoop_enabler] Error parsing dfsadmin report line number [" + 
                                                           str(lineNumber) + "].  Decommissioned host does not have a name.")
                else:
                    decommissionedHostList.append(prevName)
                    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Decommissioned host found [" + str(prevName) + "].")

            else:
                if (status.startswith("Decommission in progress")):
                    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Decommission in progress on node [" + str(prevName) + "].") 
                prevName == None

    proc.wait()
      
    if (proc.returncode == 0):
        ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] dfsadmin report successfully ran.")
    else:
        ContainerUtils.getLogger(proxy).severe("[hadoop_enabler] Unexpected return code [" + str(proc.returncode) + "] while attempting to generate dfsadmin report.")
        
    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Exiting getDecommssionedHosts()") 
    return decommissionedHostList
        
def refreshNodes():
    
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Refreshing Hadoop Host exclusions")
    hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()
    runCommand("sh " + os.path.join(hadoop_bin_dir, "hadoop") + " dfsadmin -refreshNodes", suppressOutput=True)   
"""
def doInitOptionalRCV(varname, default, additionalVariables):
    
    var = proxy.getContainer().getRuntimeContext().getVariable(varname)
    if (var == None):
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Runtimecontext Vaiable ${" + varname + 
                                                "} is not defined.  Setting to [" + str(default) + "].")
        additionalVariables.add(RuntimeContextVariable(varname, str(default),  RuntimeContextVariable.STRING_TYPE))
        value = str(default) 
    else:
        value =  var.getValue()
    
    return value
"""
""" **********************************************************
Datanode Functions
********************************************************** """

def decomreq_file():
    #host = socket.gethostname()
    #engineinstance = proxy.getContainer().getRuntimeContext().getVariable('ENGINE_INSTANCE').getValue()
    #hadop_home_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_HOME_DIR').getValue()
    port = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_DATANODE_BASEPORT').getValue()
    return os.path.join(decommissionqueue_dir, hostip + "-" + str(port))

def recomreq_file():
    #host = socket.gethostname()
    #engineinstance = proxy.getContainer().getRuntimeContext().getVariable('ENGINE_INSTANCE').getValue()
    #hadop_home_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_HOME_DIR').getValue()
    port = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_DATANODE_BASEPORT').getValue()
    return os.path.join(recommissionqueue_dir, hostip + "-" + str(port))

def balancerreq_file():
    #host = socket.gethostname()
    #engineinstance = proxy.getContainer().getRuntimeContext().getVariable('ENGINE_INSTANCE').getValue()
    #hadop_home_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_HOME_DIR').getValue()
    port = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_DATANODE_BASEPORT').getValue()
    return os.path.join(balancerqueue_dir, hostip + "-" + str(port))

def isHadoopShuttingDown():

    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Beginning isHadoopShuttingDown().")
    shutDownDetected = False
    try:

        admin = AdminManager.getStackAdmin()
        #throws Exception -IllegalStateException - If called on an Engine but the Engine is not yet allocated to run a Component
        componentAllocationMap  = admin.getComponentAllocationMap()
        #throws Exception - Exception - if effective policy is unavailable

        namenodecomponent = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_NAMENODE_COMPONENT').getValue()        
        namenodeexpectedengines = getComponentExpectedEngined(namenodecomponent, componentAllocationMap)
        if (namenodeexpectedengines == 0):  

            curcomponent = getCurComponentName()
            curnodeexpectedengines = getComponentExpectedEngined(curcomponent, componentAllocationMap)
            # Checking the current node engine count to be sure
            if (curnodeexpectedengines == 0):
                
                ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] Hadoop shutdown detected.  Expected Engine Count for Namenode and Datanode comnponents are 0.")
                shutDownDetected = True
                
    except:
       ContainerUtils.getLogger(proxy).severe("[hadoop_enabler] Unexpected error trying to get to determine status HDFS components (namenode and datanode).  "  +
                                              "Will assume HDFS is still in running mode.")
       traceback.print_exc()
                
    ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Exiting isHadoopShuttingDown().  Return is [" + str(shutDownDetected) + "]")
    return shutDownDetected 

def getComponentExpectedEngined(componentName, componentAllocationMap):

    try:
        componentAllocationEntryInfo = componentAllocationMap.getAllocationEntry(componentName)

        if (componentAllocationEntryInfo == None):
            expectedEngineCount = 0
        else:
            expectedEngineCount = int(componentAllocationEntryInfo.getExpectedEngineCount()) 
        
        ContainerUtils.getLogger(proxy).fine("[hadoop_enabler] Expected Engine Count for component [ " + str(componentName) + " ] is [" + str(expectedEngineCount) + "]")
        
    except:
       ContainerUtils.getLogger(proxy).severe("[hadoop_enabler] Unexpected error trying to get Expected Engine count for component [" + str(componentName) + 
                                              "].  Will assume it is still in a running mode.")
       traceback.print_exc()
       
       expectedEngineCount = -1

    return expectedEngineCount

def doStart_datanode():
    
    hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()
    
    pendingRequestsCleared = False

    decommissionrequest_file = decomreq_file()    

    if (hdfsFileExists(decommissionrequest_file) == 1): 
        
        recommissionrequest_file = recomreq_file()
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Adding request to recommission queue: [" + recommissionrequest_file + "]");
    
        commandline = os.path.join(hadoop_bin_dir, "hadoop") + " fs -put - " + recommissionrequest_file            
    
        recommissionrequest_content = str(datetime.datetime.now())          
        if getHadoopVersion().startswith("0.23"):
            output = runCommand(commandline, stdin=recommissionrequest_content, expectedReturnCodes=[0,1])
        else:
            output = runCommand(commandline, stdin=recommissionrequest_content, expectedReturnCodes=[0,255])
    
        if (output[0] == 0) or (output[0] == 255) or (output[0] == 1):
            pollperiod = getContainerRunningConditionPollPeriod()/1000
            while (True):
                
                ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Waiting for recommission request to processed.  Will check again in [" + str(pollperiod) + "] seconds")
                time.sleep(pollperiod)                                
                
                if (hdfsFileExists(recommissionrequest_file) == 0):   
                    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Recommission request has been processed.  Continuing with startup")
                    pendingRequestsCleared = True
                    break
                
                if (isHadoopShuttingDown()):
                    ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] Datanode startup interrupted by Hadoop shutdown.")
                    raise Exception("[hadoop_enabler] Datanode startup interrupted by Hadoop shutdown.")
                    break
        else:
            ContainerUtils.getLogger(proxy).severe("[hadoop_enabler] Unexpected return code [" + str(output[0]) + "] while tryiing to post recommission request.  Unable to proceed with datanode restart.")

    else:
        pendingRequestsCleared = True

    if pendingRequestsCleared:
        
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Starting Hadoop Datanode Daemon")
        startDaemon("datanode", 'hadoop_enabler_DATANODE_PID')                    

        balancerrequest_file = balancerreq_file()
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Adding request to balancer queue: [" + balancerrequest_file + "]");
    
        commandline = os.path.join(hadoop_bin_dir, "hadoop") + " fs -put - " + balancerrequest_file            
    
        balancerrequest_content = str(datetime.datetime.now())          
        if getHadoopVersion().startswith("0.23"):
            runCommand(commandline, stdin=balancerrequest_content, expectedReturnCodes=[0,1], suppressOutput=True)            
        else:
            runCommand(commandline, stdin=balancerrequest_content, expectedReturnCodes=[0,255], suppressOutput=True)            
 
def doShutDown_datanode():

    hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()        

    hadoop_tmp_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_TMP_DIR').getValue()
    pid_file_name = os.path.join(hadoop_tmp_dir, "hadoop-" + getpass.getuser() + "-datanode.pid") 

    shutdowncomplete = False

    if (isHadoopShuttingDown()):        
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Hadoop is shutting down.")
    elif not (os.path.isfile(pid_file_name)):
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Unable to find pid file [" + pid_file_name + "]")            
    else:

        pid_file = open(pid_file_name, 'r')
        pid = pid_file.readline().rstrip('\n')
        
        file = decomreq_file()
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] adding entry to decommission queue: [" + file + "]");

        if ContainerUtils.isWindows():
            raise Exception("[hadoop_enabler] Windows not yet implemented in by this enabler.")
        else:
            commandline = os.path.join(hadoop_bin_dir, "hadoop") + " fs -put - " + file            

        decommissionrequest_content = str(pid)          
        output = runCommand(commandline, stdin=decommissionrequest_content)

        if (output[0] == 0):
            pollperiod = getContainerRunningConditionPollPeriod()/1000
            while (True):
                ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Waiting for daemon proccess [" + str(pid) + 
                                                     "] to be terminated.  Will check again in [" + str(pollperiod) + "] seconds")
                time.sleep(pollperiod)                                

                commandline = "ps h -o pid,command -p " + pid
                output = runCommand(commandline, stdout=PIPE, expectedReturnCodes=[0,1], suppressOutput=True)
                if (output[1] == ""):
                    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Datanode daemon has been terminated.  Continuing with shutdown")
                    shutdowncomplete = True
                    break
                elif (isHadoopShuttingDown()):       
                    ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] Interrupting wait for process [" + 
                                                         pid + "] to be terminated.")
                    break

    if not (shutdowncomplete):
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Stopping Hadoop Datanode Daemon Immediately")
        stopDaemon("datanode")                        
                
def hdfsFileExists(file):
    hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()
    
    commandline = os.path.join(hadoop_bin_dir, "hadoop") + " fs -ls " + file
    #Older versions of hadoop (e.g 1.0.4 and 1.1.2) return 255 if file does not exist.  Newer versions return 1             
    output = runCommand(commandline, stdout=PIPE, expectedReturnCodes=[0, 255, 1], suppressOutput=True)

    if (output[0] == 0):
        fileExists = 1
    elif (output[0] == 255) or (output[0] == 1):
        fileExists = 0
    else:
        ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] Unexpected return code " + str(output[0]) + 
                                                " while trying to test for existence of [" + str(file) + "].")
        fileExists = -1

    return fileExists
    
