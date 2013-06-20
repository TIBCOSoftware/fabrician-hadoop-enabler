/*
 * Copyright (c) 2013 TIBCO Software Inc. All Rights Reserved.
 * 
 * Use is subject to the terms of the TIBCO license terms accompanying the download of this code. 
 * In most instances, the license terms are contained in a file named license.txt.
 */
package org.fabrician.enabler.hadoop;

public class HadoopJobtrackerMBeanServerRef extends HadoopMBeanServerRef {

    public String getEnableFlagVar() {
        return "hadoop_enabler_ENABLE_JOBTRACKER";
    }

    public String getJMXBasePortVar() {
        return "hadoop_enabler_JOBTRACKER_JMX_BASEPORT";
    }

}
