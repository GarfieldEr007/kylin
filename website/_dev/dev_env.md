---
layout: dev
title:  Setup Development Env
categories: development
permalink: /development/dev_env.html
---

Developers want to run kylin test cases or applications at their development machine. 

By following this tutorial, you will be able to build kylin test cubes by running a specific test case, and you can further run other test cases against the cubes having been built.

## Environment on the Hadoop CLI

Off-Hadoop-CLI installation requires you having a hadoop CLI machine (or a hadoop sandbox) as well as your local develop machine. To make things easier we strongly recommend you starting with running Kylin on a hadoop sandbox, like <http://hortonworks.com/products/hortonworks-sandbox/>. In the following tutorial we'll go with **Hortonworks Sandbox 2.2.4**. It is recommended that you provide enough memory to your sandbox, 8G or more is preferred.

### Start Hadoop

In Hortonworks sandbox, ambari helps to launch hadoop:

{% highlight Groff markup %}
ambari-agent start
ambari-server start
{% endhighlight %}
	
With both command successfully run you can go to ambari home page at <http://yoursandboxip:8080> (user:admin,password:admin) to check everything's status. By default ambari disables Hbase, you'll need manually start the `Hbase` service.

For other hadoop distribution, basically start the hadoop cluster, make sure HDFS, YARN, Hive, HBase are running.


## Environment on the dev machine


### Install maven

The latest maven can be found at <http://maven.apache.org/download.cgi>, we create a symbolic so that `mvn` can be run anywhere.

{% highlight Groff markup %}
cd ~
wget http://xenia.sote.hu/ftp/mirrors/www.apache.org/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz
tar -xzvf apache-maven-3.2.5-bin.tar.gz
ln -s /root/apache-maven-3.2.5/bin/mvn /usr/bin/mvn
{% endhighlight %}

### Compile

First clone the Kylin project to your local:

{% highlight Groff markup %}
	git clone https://github.com/apache/kylin.git
{% endhighlight %}
	
Install Kylin artifacts to the maven repo

{% highlight Groff markup %}
	mvn clean install -DskipTests
{% endhighlight %}

### Modify local configuration

Local configuration must be modified to point to your hadoop sandbox (or CLI) machine. 

* In **examples/test_case_data/sandbox/kylin.properties**
   * Find `sandbox` and replace with your hadoop hosts (if you're using HDP sandbox, this can be skipped)
   * Fine `kylin.job.run.as.remote.cmd` and change it to "true" (in code repository the default is false, which assume running it on hadoop CLI)
   * Find `kylin.job.remote.cli.username` and `kylin.job.remote.cli.password`, fill in the user name and password used to login hadoop cluster for hadoop command execution; If you're using HDP sandbox, the default username is "root" and password is "hadoop".

* In **examples/test_case_data/sandbox**
   * For each configuration xml file, find all occurrence of `sandbox` and replace with your hadoop hosts; (if you're using HDP sandbox, this can be skipped)

An alternative to the host replacement is updating your `hosts` file to resolve `sandbox` and `sandbox.hortonworks.com` to the IP of your sandbox machine.

### Run unit tests

Run a end-to-end cube building test, these special test cases will populate some sample cubes in your metadata store and build them ready.
It might take a while (maybe one hour), please keep patient.
 
{% highlight Groff markup %}
	mvn test -Dtest=org.apache.kylin.job.BuildCubeWithEngineTest -DfailIfNoTests=false -Dhdp.version=<hdp-version> -P sandbox
	
	mvn test -Dtest=org.apache.kylin.job.BuildIIWithEngineTest -DfailIfNoTests=false -Dhdp.version=<hdp-version> -P sandbox
{% endhighlight %}
	
Run other tests, the end-to-end cube building test is exclueded

{% highlight Groff markup %}
	mvn test -fae -P sandbox
{% endhighlight %}

### Launch Kylin Web Server

Copy server/src/main/webapp/WEB-INF to webapp/app/WEB-INF 

{% highlight Groff markup %}
	cp -r server/src/main/webapp/WEB-INF webapp/app/WEB-INF 
{% endhighlight %}

Download JS for Kylin web GUI. `npm` is part of `Node.js`, please search about how to install it on your OS.

{% highlight Groff markup %}
	cd webapp
	npm install -g bower
	bower --allow-root install
{% endhighlight %}

In IDE, launch `org.apache.kylin.rest.DebugTomcat` with working directory set to the /server folder. (By default Kylin server will listen on 7070 port; If you want to use another port, please specify it as a parameter when run `DebugTomcat)

Check Kylin Web available at http://localhost:7070/kylin (user:ADMIN,password:KYLIN)

For IntelliJ IDEA users, need modify "server/kylin-server.iml" file, replace all "PROVIDED" to "COMPILE", otherwise an "java.lang.NoClassDefFoundError: org/apache/catalina/LifecycleListener" error may be thrown;


## Setup IDE code formatter

In case you're writting code for Kylin, you should make sure that your code in expected formats.

For Eclipse users, just format the code before committing the code.

For intellij IDEA users, you have to do a few more steps:

1. Install "Eclipse Code Formatter" and configure it as follows:

	![Eclipse_Code_Formatter_Config](/images/develop/eclipse_code_formatter_config.png)

2. Disable intellij IDEA's "Optimize imports on the fly"

	![Disable_Optimize_On_The_Fly](/images/develop/disable_import_on_the_fly.png)

3. Format the code before committing the code.
