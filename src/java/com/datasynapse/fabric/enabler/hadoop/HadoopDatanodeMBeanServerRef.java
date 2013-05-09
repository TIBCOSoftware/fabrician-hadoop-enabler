package org.fabrician.enabler.hadoop;

public class HadoopDatanodeMBeanServerRef extends HadoopMBeanServerRef {

    public String getenbale_flag_var() {
    	return "hadoop_enabler_ENABLE_DATANODE";
    }

	public String getjmx_baseport_var() {
		return "hadoop_enabler_DATANODE_JMX_BASEPORT";
	}

}
