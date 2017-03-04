#!/bin/bash

java -Djava.util.logging.SimpleFormatter.format='%1$tY/%1$tm/%1$td %1$tH:%1$tM:%1$tS [%4$s] %5$s%n' Crawler --host localhost --port 1527
