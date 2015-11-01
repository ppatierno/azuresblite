# Azure SB Lite

![](images/azuresblite.png)

Access to Microsoft Azure Service Bus from all platforms using AMQP protocol

*Project Description*

This project has the purpose to provide all the same APIs to access the Microsoft Azure Service Bus (Queues, Topics/Subscriptions, Event Hubs) you have on .Net Framework but for .Net Micro Framework, .Net Compact Framework 3.9, Mono (on Linux) and WinRT (Windows 8.1 and Windows 10) using AMQP protocol.

It's based on the [AMQP .Net Lite library](https://github.com/Azure/amqpnetlite) and makes a wrapper around it so that developers don't need to know about AMQP protocol concepts like connection, session and links but can access to the Microsoft Azure Service Bus in the same way they do on applications for PC. 

It is developed in C# language and works on all the following *.Net platforms* :

* .Net Framework (up to 4.5) 
* .Net Compact Framework 3.9 (for Windows Embedded Compact 2013)
* .Net Micro Framework 4.3 
* Mono (for Linux O.S.)

There is also the support for *WinRT* platforms :

* Windows 8.1 and Windows 10

For more information :

* Azure Service Bus:  http://msdn.microsoft.com/en-us/library/ee732537.aspx
* Azure Service Bus and AMQP:  http://msdn.microsoft.com/en-us/library/jj841071.aspx
* Azure Service Bus Event Hub:  http://azure.microsoft.com/en-us/services/event-hubs/
* AMQP:  http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-overview-v1.0-os.html

Follow the project on Twitter at [@azuresblite](https://twitter.com/azuresblite)

You can find some example scenarios on GitHub at following address :

[Azure SB Lite Examples](https://github.com/ppatierno/azuresblite-examples)

* Simple send to Event Hub
* Send to Event Hub to a specific partition
* Send to Event Hub to a publisher
* Send to Event Hub to a publisher with token
* Send to Event Hub with a partition key
* Receive from Event Hub from specific partition
* Receive from Event Hub from specific partition with offset
* Receive from Event Hub from specific partition with date/time offset
* Simple send to a Queue
* Send to a Queue and receive from a ReplyTo Queue
* Send to a Topic and receive from Sunscriptions
