#!/bin/bash
export ORCL_USERNAME=username
export ORCL_PASSWORD=password
export ORCL_DBURL=127.0.0.1:1521/orcl
nohup python tornado_server.py &
