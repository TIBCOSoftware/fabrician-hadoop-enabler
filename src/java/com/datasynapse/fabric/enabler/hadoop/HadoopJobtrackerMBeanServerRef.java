package org.fabrician.enabler.hadoop;

public class HadoopJobtrackerMBeanServerRef extends HadoopMBeanServerRef {

    public String getenbale_flag_var() {
    	return "hadoop_enabler_ENABLE_JOBTRACKER";
    }

	public String getjmx_baseport_var() {
		return "hadoop_enabler_JOBTRACKER_JMX_BASEPORT";
	}

}
