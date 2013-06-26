from com.datasynapse.fabric.admin.info import AllocationInfo
from com.datasynapse.fabric.util import GridlibUtils, ContainerUtils
from com.datasynapse.fabric.common import RuntimeContextVariable, ActivationInfo
from com.datasynapse.fabric.admin import AdminManager
from com.datasynapse.fabric.admin.info import GridlibInfo

from xml.dom import minidom
from jarray import array
from java.lang.management import ManagementFactory
from subprocess import Popen, PIPE, STDOUT, call
import inspect
import os
import errno
import signal
import shlex
import traceback
import datetime

balancerqueue_dir = "/.fabric/balancer-queue/"

reservedproperties = ["fs.default.name",
                      "hadoop.tmp.dir",
                      "topology.script.file.name",
                      "dfs.http.address",
                      "dfs.secondary.http.address",
                      "dfs.datanode.address",
                      "dfs.datanode.http.address",
                      "dfs.datanode.ipc.address",
                      "dfs.hosts.exclude",
                      "dfs.name.dir",
                      "dfs.data.dir",
                      "mapred.job.tracker",
                      "mapred.job.tracker.http.address",
                      "mapred.task.tracker.http.address"
                      ]

try: proxy
except NameError:    
    globals()['proxy'] = inspect.currentframe().f_back.f_globals['proxy']
else: pass
logger = ContainerUtils.getLogger(proxy)

def getDynamicGridlibDependencies():
    logger.info("[hadoop_enabler_common] Beginning getDynamicGridlibDependencies()")

    hadoopVersion = getHadoopVersion()
    logger.info("[hadoop_enabler_common] Hadoop Distribution version is [" + str(hadoopVersion) +"]")    

    defaultDomainGridlib = GridlibInfo()
    defaultDomainGridlib.name = "default-domain-type"
    
    logger.info("[hadoop_enabler_common] Adding Hadoop distribution dependency")
    gridlib = GridlibInfo()
    gridlib.name = "hadoop-distribution"
    gridlib.version = str(hadoopVersion)
    
    logger.info("[hadoop_enabler_common] Exiting getDynamicGridlibDependencies()")
    return array([gridlib, defaultDomainGridlib], GridlibInfo)

def getHadoopVersion():

    hadoopVersionVar = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_DISTRIBUTION_VERSION')
    if hadoopVersionVar == None:  
        logger.warning("[hadoop_enabler_common] ${hadoop_enabler_DISTRIBUTION_VERSION} is not set.  Defaulting to Hadoop Version 1.0.4")
        hadoopVersion = "1.0.4"
    else:
        hadoopVersion = hadoopVersionVar.getValue()    
        
    return str(hadoopVersion)

def doInit_common(additionalVariables):
    
    workdir = proxy.getContainer().getRuntimeContext().getVariable('CONTAINER_WORK_DIR').getValue()    
    hadoopVersion = getHadoopVersion()    
    
    distributionDir = "hadoop-" + str(hadoopVersion)
    hadoop_home_dir = os.path.join(workdir, distributionDir)
    additionalVariables.add(RuntimeContextVariable("hadoop_enabler_HADOOP_HOME_DIR", hadoop_home_dir, RuntimeContextVariable.STRING_TYPE))
    additionalVariables.add(RuntimeContextVariable("hadoop_enabler_HADOOP_BIN_DIR", os.path.join(hadoop_home_dir, "bin"), RuntimeContextVariable.STRING_TYPE))                                                   
    additionalVariables.add(RuntimeContextVariable("hadoop_enabler_HADOOP_SBIN_DIR", os.path.join(hadoop_home_dir, "sbin"), RuntimeContextVariable.STRING_TYPE))                                                   
    additionalVariables.add(RuntimeContextVariable("hadoop_enabler_HADOOP_CONF_DIR", os.path.join(hadoop_home_dir, "conf"), RuntimeContextVariable.ENVIRONMENT_TYPE))                                                   

    """ Augment the Enablers's Hadoop Configuration files with values from user-supplied Hadoop Configuration files."""
    doInitHadoopProps("hadoop_enabler_USERPROPS_CORE", "hadoop_enabler_USERPROPS_CORE_FILE", additionalVariables)
    doInitHadoopProps("hadoop_enabler_USERPROPS_HDFS", "hadoop_enabler_USERPROPS_HDFS_FILE", additionalVariables)
    doInitHadoopProps("hadoop_enabler_USERPROPS_MAPRED", "hadoop_enabler_USERPROPS_MAPRED_FILE", additionalVariables)

    """ Create Hadoop tmp directory if it does not already exist"""
    tmpdir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_TMP_DIR').getValue()    
    try:
        os.makedirs(tmpdir)
    except OSError, exc:
        if exc.errno == errno.EEXIST and os.path.isdir(tmpdir):
            pass
        else: 
            raise 

def doInitHadoopProps(userProp_RCVname, userPropFile_RCVname, additionalVariables):

    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Checking for user property file to augment [" + str(userProp_RCVname) + "].")
    
    userPropsRCV = proxy.getContainer().getRuntimeContext().getVariable(userProp_RCVname)
    userPropsPredefined = False
    if (userPropsRCV == None):
        userProps = ""         
    else:
        userPropsPredefined = True
        userProps =  userPropsRCV.getValue()
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] User properties variable ${" + userProp_RCVname + 
                                                "} is was imported or pre-defined on component.  Starting value is [" + str(userProps) + "].")

    userPropFile = proxy.getContainer().getRuntimeContext().getVariable(userPropFile_RCVname).getValue()

    if (userPropFile != "") and os.path.isfile(userPropFile):
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] User property file found [" + str(userPropFile) + "].")
        xmldoc = minidom.parse(userPropFile)
        propertylist = xmldoc.getElementsByTagName('property')
    
        if propertylist == None or len(propertylist) == 0:
            ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] No property elements found in user property file.")
        else:
            for element in propertylist:
                nameElements = element.getElementsByTagName("name")
                name = getNodeText(nameElements[0])
    
                isReserved = False                
                for reservedproperty in reservedproperties:
                    if reservedproperty.count(name) > 0:
                       isReserved = True
                       break
                
                if isReserved:
                    ContainerUtils.getLogger(proxy).warning("[hadoop_enabler] The property [" + str(name) + "] is managed by the Hadoop Enabler.  Will ignore user supplied value.")
                else:    
                    ContainerUtils.getLogger(proxy).info("[hadoop_enabler] Applying user property [" + str(element.toxml()) + "].")
                    userProps = userProps + element.toxml()
    
    if userPropsPredefined:
        proxy.getContainer().getRuntimeContext().getVariable(userProp_RCVname).setValue(userProps)
    else:
        additionalVariables.add(RuntimeContextVariable(userProp_RCVname, userProps, RuntimeContextVariable.STRING_TYPE,"User Supplied Hadoop properties" , False, RuntimeContextVariable.NO_INCREMENT))

"""
def getHadoopConfigParameter(file, propertyname):
    
    from xml.dom import minidom
    xmldoc = minidom.parse(file)
    propertylist = xmldoc.getElementsByTagName('property')
    value = []
    for element in propertylist:
        nameElements = element.getElementsByTagName("name")
        name = getNodeText(nameElements[0])
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler] DEBUG name = [" + str(name) + "]")
        if name == propertyname:
            valueElements = element.getElementsByTagName("value")
            valueCurNode = getNodeText(valueElements[0])
            ContainerUtils.getLogger(proxy).info("[hadoop_enabler] DEBUG valueCurNode = [" + str(valueCurNode) + "]")
            value.append(valueCurNode)

    return ''.join(value)
"""
def getNodeText(node):
    nodelist = node.childNodes
    value = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            value.append(node.data)
    return ''.join(value)


def doStart_common():
        
    moveContentFiles()
    
    createEnvironmentScript()
    
    killOrphans()

    changePermissions()        

def moveContentFiles():
    
    hadoop_home_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_HOME_DIR').getValue()
    work_dir = proxy.getContainer().getRuntimeContext().getVariable('CONTAINER_WORK_DIR').getValue()
    
    if ContainerUtils.isWindows():
        pass
    else:
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_common] Copying enabler content files to version-specific sub-directory.")       
        runCommand("cp -rv " + work_dir + "/hadoop-content/* " + hadoop_home_dir, shell=True)
#        commandline = "cp -r " + work_dir + "/hadoop-content/* " + hadoop_home_dir 
#        runCommand(commandline)
    
def rcvTrue(rcv):
    
    ContainerUtils.getLogger(proxy).finer("[hadoop_enabler_common] checking runtimecontext variable [" + str(rcv) + "]")
    
    rcvvalue = proxy.getContainer().getRuntimeContext().getVariable(rcv).getValue()
    ContainerUtils.getLogger(proxy).finest("[hadoop_enabler_common] value is [" + str(rcvvalue) + "].")
    if (str(rcvvalue).lower() in ("yes", "y", "true",  "t", "1")): 
        result = True
    elif (str(rcvvalue).lower() in ("no",  "n", "false", "f", "0")): 
        result = False
    else:
        raise Exception("[hadoop_enabler_common] Invalid value for boolean conversion: [" + str(rcvvalue) + "]")
    ContainerUtils.getLogger(proxy).finer("[hadoop_enabler_common] Exiting Checking enabler flag. Result is [" + str(result) + "]")
    return result

def getScript(script):
    
    hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()        
    hadoop_sbin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_SBIN_DIR').getValue()        
    
    hadoopDaemonScript_bin = os.path.join(hadoop_bin_dir, script)
    hadoopDaemonScript_sbin = os.path.join(hadoop_sbin_dir, script)
    if os.path.isfile(hadoopDaemonScript_bin):
        return hadoopDaemonScript_bin
    elif os.path.isfile(hadoopDaemonScript_sbin):
        return hadoopDaemonScript_sbin
    else:
        raise Exception("[hadoop_enabler_common] Unable to locate [" + script + "] in Hadoop distribution.")

def runCommand(commandline, stdin=None, stdout=None, expectedReturnCodes=None, suppressOutput=None, shell=None):

    if (expectedReturnCodes == None): expectedReturnCodes = [0]
    if (suppressOutput == None): suppressOutput = False
    if (shell == None): shell = False
    stderr = None
    if (suppressOutput):
        stdout=PIPE
        stderr=PIPE
    else: 
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_common] Running command [" + commandline + "]")
            
    if shell:
        args = commandline
    else:
        args = shlex.split(commandline)

    os.unsetenv("LD_LIBRARY_PATH")
    os.unsetenv("LD_PRELOAD")

    if stdin == None:
        p = Popen(args, stdout=stdout, stdin=None, stderr=stderr, shell=shell)
        output = p.communicate()        
    else:
        p = Popen(args, stdout=stdout, stdin=PIPE, stderr=stderr, shell=shell)
        output = p.communicate(input=stdin)
    
    outputlist = [p.returncode]

    for item in output:
        outputlist.append(item)

    if (outputlist[0] in expectedReturnCodes ):
        if not (suppressOutput):
            ContainerUtils.getLogger(proxy).info("[hadoop_enabler_common] Command return code was [" + str(outputlist[0]) + "]")
            printStdoutPipe(stdout, outputlist)
    else:
        
        ContainerUtils.getLogger(proxy).severe("[hadoop_enabler_common] Return code " + str(outputlist[0]) + 
                                               " was not in list of expected return codes" + str(expectedReturnCodes))
        if (suppressOutput):
            ContainerUtils.getLogger(proxy).severe("[hadoop_enabler_common] Command was [" + commandline + "]")

        printStdoutPipe(stdout, outputlist)

    ContainerUtils.getLogger(proxy).finer("[hadoop_enabler_common] exiting runCommand(). Returning outputlist:" + (str(outputlist)))
    return outputlist

def printStdoutPipe(stdout, outputlist):

    if (stdout == PIPE):
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_common] Command STDOUT:")
        print outputlist[1]
         

        
def killOrphans():     
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler_common] Killing any orphaned process on this engine remaining from a previous execution")
    
    hadoop_home_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_HOME_DIR').getValue()

    if ContainerUtils.isWindows():
        raise Exception("[hadoop_enabler_common] Windows not yet implemented in by this enabler.")
    else:
        commandline = "ps x"
    output = runCommand(commandline, expectedReturnCodes=[0, 255], stdout=PIPE, suppressOutput=True)

    for line in output[1].splitlines():
        if hadoop_home_dir in line:
            if "java" in line:
                ContainerUtils.getLogger(proxy).info("[hadoop_enabler_common] Issuing Kill command for orphaned process [" + str(line) + "]")
                pid = int(line.split(None, 1)[0])
                os.kill(pid, signal.SIGKILL)

def deleteHDFSFile(file): 

    hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()        

    commandline = os.path.join(hadoop_bin_dir, "hadoop") + " fs -rm " + str(file)
    output = runCommand(commandline, expectedReturnCodes=[0, 255])

    if (output[0] == 0):
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_common] Successfully deleted [" + str(file) + "]")            
    elif (output[0] == 255):
        ContainerUtils.getLogger(proxy).warning("[hadoop_enabler_common] File already deleted [" + str(file) + "]. Continuing Processing")
    else:            
        ContainerUtils.getLogger(proxy).severe("[hadoop_enabler_common] Unexpected return code [" + str(output[0]) + "] when attempting to delete.]")

def getStatistic_common(name):

    memoryBean = ManagementFactory.getMemoryMXBean()

    if name == "enabler_HEAP_MEMORY_USAGE":
        return memoryBean.getHeapMemoryUsage().getUsed()
    elif name == "enabler_NON_HEAP_MEMORY_USAGE":
        return memoryBean.getNonHeapMemoryUsage().getUsed()
    
    elif name == "enabler_DATANODE_DECOMMISION_REQUESTS":
        
        hadoop_home_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_HOME_DIR').getValue()        

        if ContainerUtils.isWindows():
            raise Exception("[hadoop_enabler_common] Windows not yet implemented in by this enabler.")
        else:
            commandline = "sh " + os.path.join(hadoop_home_dir, "bin", "hadoop") + " fs -count " + decommissionqueue_dir
            
        output = runCommand(commandline, expectedReturnCodes=[0, 255], suppressOutput=True)
        if (output[0] == 0): 
            stdout = str(output[1])
            count = int(stdout.split()[1])
            return int(count)
        elif (output[0] == 255):
            #Decommission request directoy doesn't exist.  Not expected to exist until the some datanode posts the first request  
            return int(0)    
        else:
            ContainerUtils.getLogger(proxy).warning("[hadoop_enabler_common] Unexpected return code [" + str(output[0]) + 
                                                    "] while attempting to retrieve statistic enabler_DATANODE_DECOMMISION_REQUESTS statistic.  Assuming 0.")
            print output
            return int(0)
        
    elif name.startswith('enabler_DISK_'):

        tmpdir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_TMP_DIR').getValue()    
        
        if name == "enabler_DISK_SPACE_FREE":
            return getStatistic_disk(tmpdir)[0]
            #return available
        elif name == "enabler_DISK_SPACE_USED":
            return getStatistic_disk(tmpdir)[1]
            #return used
        elif name == "enabler_DISK_SPACE_USED_PERCENT":
            return getStatistic_disk(tmpdir)[2]
            #return int(percent[:-1])
        else:
            raise Exception("[hadoop_enabler_common] Unsupported statistic type requested [" + str(name) + "]")

    else:
        raise Exception("[hadoop_enabler_common] Unsupported statistic type requested [" + str(name) + "]")


def getStatistic_disk(directory):
    if ContainerUtils.isWindows():
        raise Exception("[hadoop_enabler_common] Windows not yet implemented in by this enabler.")
        return[0,0,0]
    else:
        
        df = Popen(["df", directory], stdout=PIPE)
        output = df.communicate()[0]
        device, size, used, available, percent, mountpoint = output.split("\n")[1].split()
        
        return [available, used, int(percent[:-1])]

def getContainerRunningConditionPollPeriod():
    pollperiod = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_ENABLER_RUNNING_POLLPERIOD').getValue()
    ContainerUtils.getLogger(proxy).info("[hadoop_enabler_common] Will verify enabler is running every " + str(pollperiod) + " seconds.")
    return float(pollperiod) * 1000

def getComponentRunningConditionErrorMessage():
    return "hadoop heartbeat test unsuccessful"

def changePermissions():

    hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()
    hadoop_sbin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_SBIN_DIR').getValue()
    java_home_dir = proxy.getContainer().getRuntimeContext().getVariable('GRIDLIB_JAVA_HOME').getValue()
    
    if ContainerUtils.isWindows():
        pass
    else:
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_common] Changing file permissions on directory " + hadoop_bin_dir)       
        commandline = "chmod -Rv u+x " + hadoop_bin_dir
        runCommand(commandline)
        
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_common] Changing file permissions on directory " + hadoop_sbin_dir)       
        commandline = "chmod -Rv u+x " + hadoop_sbin_dir
        runCommand(commandline)
        
        java_bin_dir = os.path.join(java_home_dir, "bin")
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_common] Changing file permissions on directory " + java_bin_dir)       
        commandline = "chmod -Rv u+x " + java_bin_dir
        runCommand(commandline)
        
        java_jrebin_dir = os.path.join(java_home_dir, "jre", "bin")
        ContainerUtils.getLogger(proxy).info("[hadoop_enabler_common] Changing file permissions on directory " + java_jrebin_dir)       
        commandline = "chmod -Rv u+x " + java_jrebin_dir
        runCommand(commandline)
        
def createEnvironmentScript():
    
    hadoop_bin_dir = proxy.getContainer().getRuntimeContext().getVariable('hadoop_enabler_HADOOP_BIN_DIR').getValue()
    
    if ContainerUtils.isWindows():
        environmentFilename = os.path.join(hadoop_bin_dir, "hadoop_enabler_ENVIRONMENT.bat")
    else:
        environmentFilename = os.path.join(hadoop_bin_dir, "hadoop_enabler_ENVIRONMENT.sh")
        
    environmentFile = open(environmentFilename, 'w')
    print>>environmentFile, "###################################################"
    print>>environmentFile, "# Generated by Hodoop Enabler"
    print>>environmentFile, "#     " + str(datetime.datetime.now())
    print>>environmentFile, "#"
    print>>environmentFile, "# This file sets all the ENVIRONMENT type runtimecontext"
    print>>environmentFile, "# variables defined by this enabler."
    print>>environmentFile, "#"
    print>>environmentFile, "###################################################"

    runtimeContext = proxy.getContainer().getRuntimeContext()
    for i in range (0, (runtimeContext.getVariableCount() - 1)):
          variable = runtimeContext.getVariable(i)
          ContainerUtils.getLogger(proxy).fine("[hadoop_enabler_common] " + str(variable.getName()) + " has type [" + str(variable.getTypeInt()) + "]")
          if  (variable.getTypeInt() == RuntimeContextVariable.ENVIRONMENT_TYPE):
              print>>environmentFile, variable.getName() + "=" + str(variable.getValue())   
    
    environmentFile.close()

class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """
    def __init__(self, logger, streamtype):
        self.logger = logger
        self.streamtype = streamtype
 
    def write(self, buf):
        for line in buf.rstrip().splitlines():
            if (self.streamtype == "STDERR"):
                self.logger.warning(line.rstrip())
            else:
                self.logger.info(line.rstrip())
 

        