#!/bin/bash

FILES="python3/strict_fsh.py"
autopep8 -ia --ignore=E402,E501 ${FILES}
