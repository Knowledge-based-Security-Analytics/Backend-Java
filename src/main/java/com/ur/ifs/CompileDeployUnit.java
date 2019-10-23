/*
 ***************************************************************************************
 *  Copyright (C) 2006 EsperTech, Inc. All rights reserved.                            *
 *  http://www.espertech.com/esper                                                     *
 *  http://www.espertech.com                                                           *
 *  ---------------------------------------------------------------------------------- *
 *  The software in this package is published under the terms of the GPL license       *
 *  a copy of which has been included with this distribution in the license.txt file.  *
 ***************************************************************************************
 */
package com.ur.ifs;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.DeploymentOptions;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPUndeployException;

/*Teile dieser Klasse wurden aus dem TestPackage von EsperIO übernommen*/
class CompileDeployUnit {
    static EPDeployment compileDeploy(EPRuntime runtime, String epl) {
        try {
            Configuration configuration = runtime.getConfigurationDeepCopy();
            CompilerArguments args = new CompilerArguments(configuration);
            args.getPath().add(runtime.getRuntimePath());
            EPCompiled compiled = EPCompilerProvider.getCompiler().compile(epl, args);
            return runtime.getDeploymentService().deploy(compiled);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    static EPDeployment compileDeployWithOptions(EPRuntime runtime, String epl, DeploymentOptions options) {
        try {
            Configuration configuration = runtime.getConfigurationDeepCopy();
            CompilerArguments args = new CompilerArguments(configuration);
            args.getPath().add(runtime.getRuntimePath());
            EPCompiled compiled = EPCompilerProvider.getCompiler().compile(epl, args);
            return runtime.getDeploymentService().deploy(compiled, options);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    static void undeployAll(EPRuntime runtime) {
        try {
            runtime.getDeploymentService().undeployAll();
        } catch (EPUndeployException e) {
            throw new RuntimeException(e);
        }
    }

    static void undeployOne(EPRuntime runtime, String deploymentId) {
        try {
            runtime.getDeploymentService().undeploy(deploymentId);
        } catch (EPUndeployException e) {
            throw new RuntimeException(e);
        }
    }
}
