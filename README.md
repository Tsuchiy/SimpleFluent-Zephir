# SimpleFluent

===


Copyright (c) 2016 Nobuo Tsuchiya  


This module can only post json_serialized data to Fluentd.


###usage:

1. install zephir(https://zephir-lang.com/)

2. clone this repository

3. execute "zephir init SimpleFluent" in any directory(ex./usr/src/zephir/)

4. put souce code and execute "zephir build"

5. add setting to php.ini


###example

```
<?php
    // posting data
    $data = ["word" => "Hello World"];

    // if you use unix domain socket
    $logger1 = new \SimpleFluent\Logger("unix:///var/run/td-agent/td-agent.sock");
    $logger1->post("tag.unix", $data);

    // or use tcp    
    $logger2 = new \SimpleFluent\Logger("127.0.0.1",24224);
    $logger2->post("tag.tcp", $data);

```

##Methods

###\SimpleFluent\Logger::__construct

```
public \SimpleFluent\Logger::__construct ( string $remote_socket [, int $port [,array $options]] )
```
####parameter

######remote_socket
uri or unix domain socket

######port
tcp port number (option)

######options
array of options

+ "socket_timeout"
timeout seconds on stream(default 1 sec)

+ "connection_timeout"
Number of seconds until the connect() system call should timeout.(default 1 sec)

+ "backoff_exponential"
use exponential value to backoff(default true)

+ "backoff_wait"
backoff value when use not exponential(default 1000 microsec)

+ "persistent"
use  STREAM_CLIENT_PERSISTENT(default false)

+ "retry_socket"
retry if transmission is not complete(default true)

+ "max_write_retry"
max times to retry(default 3 times)

---

###\SimpleFluent\Logger::post

```
public \SimpleFluent\Logger::post ( string $tag, array $data )
```
####parameter


#####tag
fluentd tag

#####data
array data
