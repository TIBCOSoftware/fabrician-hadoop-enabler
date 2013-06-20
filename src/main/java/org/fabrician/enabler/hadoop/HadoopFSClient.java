/*
 * Copyright (c) 2013 TIBCO Software Inc. All Rights Reserved.
 * 
 * Use is subject to the terms of the TIBCO license terms accompanying the download of this code. 
 * In most instances, the license terms are contained in a file named license.txt.
 */
package org.fabrician.enabler.hadoop;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class HadoopFSClient {

    public HadoopFSClient() {

        super();
        // TODO Auto-generated constructor stub
    }

    public void runcommand(String command) {

        System.out.println("[HadoopFSClient] - Running command [" + command + "]");
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        System.out.println("[HadoopFSClient] - DEBUG - Currrent Context ClassLoader is:");
        System.out.println(loader);

        ClassLoader prevCl = Thread.currentThread().getContextClassLoader();

        ClassLoader urlCl = null;
        try {
            urlCl = URLClassLoader.newInstance(new URL[] { new URL("/opt/tibco") });
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.out.println("[HadoopFSClient] - DEBUG - parent of urlCL is:");
        System.out.println(urlCl.getParent());

        try {
            // Save the class loader so that you can restore it later
            Thread.currentThread().setContextClassLoader(urlCl);

            loader = Thread.currentThread().getContextClassLoader();
            System.out.println("[HadoopFSClient] - DEBUG - Updated Currrent Context ClassLoader is:");
            System.out.println(loader);

            // Expect that the environment properties are in the
            // application resource file found at "url"
            // Context ctx = new InitialContext();

            // System.out.println(ctx.lookup("tutorial/report.txt"));

            // Do something useful with ctx
            // ...
            // } catch (NamingException e) {
            // Handle the exception
        } finally {
            // Restore
            Thread.currentThread().setContextClassLoader(prevCl);
        }

        loader = Thread.currentThread().getContextClassLoader();
        System.out.println("[HadoopFSClient] - DEBUG - Restored Currrent Context ClassLoader is:");
        System.out.println(loader);

        // sun.misc.Launcher$AppClassLoader@a12a00

        // loader = ThreadClassloader.class.getClassLoader();
        // System.out.println( loader ); //
        // sun.misc.Launcher$AppClassLoader@a12a00

    }

}
