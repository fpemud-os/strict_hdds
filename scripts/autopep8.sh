#!/bin/bash

FILES=$(find ./python3 -name '*.py' | tr '\n' ' ')
autopep8 -ia --ignore=E402,E501 ${FILES}
