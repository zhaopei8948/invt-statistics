@echo off
title "invt statistics"
set ORCL_USERNAME=username
set ORCL_PASSWORD=password
set ORCL_DBURL=127.0.0.1:1521/orcl
python tornado_server.py