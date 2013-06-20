/*
 * Copyright (c) 2013 TIBCO Software Inc. All Rights Reserved.
 * 
 * Use is subject to the terms of the TIBCO license terms accompanying the download of this code. 
 * In most instances, the license terms are contained in a file named license.txt.
 */
package org.fabrician.enabler.hadoop;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.IOException;
import java.net.MalformedURLException;

import com.datasynapse.fabric.common.RuntimeContext;
import com.datasynapse.fabric.common.RuntimeContextVariable;
import com.datasynapse.fabric.container.Container;
import com.datasynapse.fabric.container.ProcessWrapper;
import com.datasynapse.fabric.domain.Domain;
import com.datasynapse.fabric.stats.MXBeanServerRef;
import com.datasynapse.fabric.util.ContainerUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class HadoopMBeanServerRef implements MXBeanServerRef {

    private transient Logger logger = ContainerUtils.getLogger(this);
    private MBeanServerConnection mBeanServerConnection = null;
    private static final ArrayList<String> TRUEVALS = new ArrayList<String>(Arrays.asList("yes", "y", "true", "t", "1"));
    private static final ArrayList<String> FALSEVALS = new ArrayList<String>(Arrays.asList("no", "n", "false", "f", "0", "0.0", "", "none", "[]", "{}"));

    public abstract String getEnableFlagVar();

    public abstract String getJMXBasePortVar();

    public void init(Container container, Domain domain, ProcessWrapper process, RuntimeContext runtimeContext) {

        logger.log(Level.INFO, "[HadoopMBeanServerRef] Beginning init()");

        if (mBeanServerConnection == null) {

            logger.log(Level.FINEST, "[HadoopMBeanServerRef] Enable flag name is [" + getEnableFlagVar() + "]");
            if (rcvTrue(getEnableFlagVar(), runtimeContext)) {

                RuntimeContextVariable port = runtimeContext.getVariable(getJMXBasePortVar());
                String url_string = "service:jmx:rmi:///jndi/rmi://" + "localhost" + ":" + port.getValue() + "/jmxrmi";

                JMXServiceURL url = null;

                try {
                    url = new JMXServiceURL(url_string);

                    JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
                    logger.log(Level.INFO, "[HadoopMBeanServerRef] Creating RMI connection to RMI connector server [" + url_string + "]");
                    mBeanServerConnection = jmxc.getMBeanServerConnection();
                } catch (MalformedURLException e) {
                    logger.log(Level.SEVERE, "[HadoopMBeanServerRef] Malformed JMX URL [" + url_string + "]");
                    e.printStackTrace();
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "[HadoopMBeanServerRef] IO Exception while trying to connect to JMX. JMX URL [" + url_string + "]");
                    e.printStackTrace();
                }
            }
        }
        logger.log(Level.INFO, "[HadoopMBeanServerRef] Exiting init()");
    }

    public boolean rcvTrue(String rcvname, RuntimeContext runtimeContext) throws IllegalArgumentException {

        logger.log(Level.FINE, "[HadoopMBeanServerRef] checking enabler flag [" + rcvname + "]");
        RuntimeContextVariable rcv = runtimeContext.getVariable(rcvname);
        String rcvvalue = (String) rcv.getValue();

        boolean result = false;

        if (TRUEVALS.contains(rcvvalue.toLowerCase())) {
            result = true;
        } else if (FALSEVALS.contains(rcvvalue.toLowerCase())) {
            result = false;
        } else {
            throw new IllegalArgumentException("[HadoopMBeanServerRef] Invalid value for boolean conversion: " + rcvvalue);
        }

        logger.log(Level.FINE, "[HadoopMBeanServerRef] Exiting checking enabler flag. Result is [" + Boolean.toString(result) + "]");

        return result;
    }

    public Object getAttribute(String bean, String attr) throws Exception {
        Object attrValue = null;
        if (mBeanServerConnection != null) {
            String[] attrs = attr.split(":"); // we do this because some
                                              // attributes (like
                                              // java.lang.Memory) are
                                              // composite and consist of
                                              // sub-attributes

            try {
                attrValue = mBeanServerConnection.getAttribute(new ObjectName(bean), attrs[0]);
                if (attrs.length > 1) {
                    for (int i = 1; i < attrs.length; ++i) {
                        attrValue = ((CompositeDataSupport) attrValue).get(attrs[i]);
                    }
                }
                logger.fine("[HadoopMBeanServerRef.getAttribute()] <" + bean + "> <" + attr + "> <" + ((Number) attrValue).doubleValue() + ">");
            } catch (Exception e) {
                // just log and ignore it while returning 0;
                logger.warning("[HadoopMBeanServerRef.getAttribute()] <" + bean + "> with attribute <" + attr + "> threw an exception.");
                logger.warning("[HadoopMBeanServerRef.getAttribute()] Exception: " + e.getMessage());
                e.printStackTrace();
                attrValue = new Double(0.0); // set value to 0
            }
            return attrValue;
        } else {
            throw new IllegalStateException("[HadoopMBeanServerRef.getAttribute()] mBeanServerConnectionhas not been initialized");
        }
    }

    public MBeanServerConnection getConnection() {
        logger.fine("[HadoopMBeanServerRef.getAttribute()] mBeanServerConnection requested");

        return this.mBeanServerConnection;
    }

}
