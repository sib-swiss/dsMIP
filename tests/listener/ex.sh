#!/bin/bash
read -s -p 'Enter password:' secret
export pass=$secret
/usr/bin/Rscript ./cat.R 
