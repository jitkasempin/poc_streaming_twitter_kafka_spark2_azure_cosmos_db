#!/usr/bin/env bash

mvn clean
echo "done clean"
mvn compile
echo "done compile"
mvn package
echo "done package"

