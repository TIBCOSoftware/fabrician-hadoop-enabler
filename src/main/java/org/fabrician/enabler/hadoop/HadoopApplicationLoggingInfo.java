/*
 * Copyright (c) 2013 TIBCO Software Inc. All Rights Reserved.
 * 
 * Use is subject to the terms of the TIBCO license terms accompanying the download of this code. 
 * In most instances, the license terms are contained in a file named license.txt.
 */
package org.fabrician.enabler.hadoop;

import com.datasynapse.fabric.domain.featureinfo.ApplicationLoggingInfo;

public class HadoopApplicationLoggingInfo extends ApplicationLoggingInfo {

    public static final String[] DEFAULT_PATTERNS = { "/hadoop-${hadoop_enabler_DISTRIBUTION_VERSION}/logs/had.*" };

    private static final long serialVersionUID = -1966013841231062199L;

    protected String[] getDefaultPatterns() {
        System.out.println(DEFAULT_PATTERNS);
        return DEFAULT_PATTERNS;
    }

}
