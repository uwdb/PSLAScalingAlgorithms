import json
import os
import sys

with open("./7workersTPCH.txt") as f:
    for string in f:
        split = string.split(" ")
        print(split[1].rstrip())
