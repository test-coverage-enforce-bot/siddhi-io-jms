siddhi-io-jms
======================================

The **siddhi-io-jms extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that used to receive and publishe events via JMS Message. This extension allows users to subscribe to a JMS broker and receive/publish JMS messages.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-jms">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-jms/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-jms/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-jms/api/1.0.22">1.0.22</a>.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

***Prerequisites for using the feature***
 - Download and install Apache ActiveMQ JMS-5.x.x.
 - Start the Apache ActiveMQ server with the following command: `bin/activemq start`.
 - Download activemq-client-5.x.x.jar (http://central.maven.org/maven2/org/apache/activemq/activemq-client/5.9.0/activemq-client-5.9.0.jar).
 - Register the InitialContextFactory implementation according to the OSGi JNDI spec and copy the client jar to the <SP_HOME>/libs directory as follows. 
   - Navigate to {WSO2SPHome}/bin and run the following command:
                   - For Linux:
                        ` ./icf-provider.sh org.apache.activemq.jndi.ActiveMQInitialContextFactory <Downloaded Jar Path>/activemq-client-5.x.x.jar <Output Jar Path>`
                   - For Windows:
                        ` ./icf-provider.bat org.apache.activemq.jndi.ActiveMQInitialContextFactory <Downloaded Jar Path>\activemq-client-5.x.x.jar <Output Jar Path>`
                 * Provide privileges if necessary using chmod +x icf-provider.(sh|bat)
   - If converted successfully then it will create 'activemq-client-5.x.x' directory in the <Output Jar Path> with OSGi converted and original jars:
                   - activemq-client-5.x.x.jar (Original Jar)
                   - activemq-client-5.x.x_1.0.0.jar (OSGi converted Jar)
                Also, following messages would be shown on the terminal
         	      - INFO: Executing 'jar uf <absolute_path>/activemq-client-5.x.x/activemq-client-5.x.x.jar -C <absolute_path>/activemq-client-5.x.x /internal/CustomBundleActivator.class'
                          [timestamp] org.wso2.carbon.tools.spi.ICFProviderTool addBundleActivatorHeader
                   - INFO: Running jar to bundle conversion [timestamp] org.wso2.carbon.tools.converter.utils.BundleGeneratorUtils convertFromJarToBundle
                   - INFO: Created the OSGi bundle activemq_client_5.x.x_1.0.0.jar for JAR file <absolute_path>/activemq-client-5.x.x/activemq-client-5.x.x.jar
   - You can find the osgi converted libs in activemq-client-5.x.x folder. You can copy 'activemq-client-5.x.x/activemq_client_5.x.x_1.0.0.jar' to {WSO2SPHome}/lib
         	   and 'activemq-client-5.x.x/activemq-client-5.x.x.jar' to {WSO2SPHome}/samples/sample-clients/lib
 - Convert and copy following jars from the <ActiveMQ_HOME>/libs directory to the <SP_HOME>/libs directory as follows.
   - Create a directory (SOURCE_DIRECTORY) in a preferred location in your machine and copy the following JARs to it from the
   <ActiveMQ_HOME>/libs directory.
     - hawtbuf-1.9.jar
     - geronimo-j2ee-management_1.1_spec-1.0.1.jar
     - geronimo-jms_1.1_spec-1.1.1.jar
   - Create another directory (DESTINATION_DIRECTORY) in a preferred location in your machine.
   - To convert all the jars you copied into the <SOURCE_DIRECTORY>, issue the following command.
     For Windows: <SP_HOME>/bin/jartobundle.bat <SOURCE_DIRECTORY_PATH> <DESTINATION_DIRECTORY_PATH>
     For Linux: <SP_HOME>/bin/jartobundle.sh <SOURCE_DIRECTORY_PATH> <DESTINATION_DIRECTORY_PATH>
   - Copy the converted files from the <DESTINATION_DIRECTORY> to the <SP_HOME>/libs directory.
   - Copy the jars that are not converted from the <SOURCE_DIRECTORY> to the <SP_HOME>/samples/sample-clients/lib directory.

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-jms/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.io.jms</groupId>
        <artifactId>siddhi-io-jms</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-jms/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-jms/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-jms/api/1.0.22/#jms-sink">jms</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink">(Sink)</a>)*<br><div style="padding-left: 1em;"><p>JMS Sink allows users to subscribe to a JMS broker and publish JMS messages.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-jms/api/1.0.22/#jms-source">jms</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>)*<br><div style="padding-left: 1em;"><p>JMS Source allows users to subscribe to a JMS broker and receive JMS messages. It has the ability to receive Map messages and Text messages.</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-jms/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-jms/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
