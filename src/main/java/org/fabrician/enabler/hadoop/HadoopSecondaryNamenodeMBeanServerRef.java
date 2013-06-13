package org.fabrician.enabler.hadoop;

public class HadoopSecondaryNamenodeMBeanServerRef extends HadoopMBeanServerRef {

    public String getenbale_flag_var() {
    	return "hadoop_enabler_ENABLE_SECONDARYNAMENODE";
    }

	public String getjmx_baseport_var() {
		return "hadoop_enabler_SECONDARYNAMENODE_JMX_BASEPORT";
	}

}
