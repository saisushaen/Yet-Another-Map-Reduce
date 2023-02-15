# from concurrent.futures import process
# from os import popen
# from sys import stderr, stdout
# from unittest import case
import subprocess

from click import command

# from subprocess import Popen,PIPE

while True:
    print(" -----------------------------")
    print("|   Select an Option          |")
    print("|   1.Read a file             |")
    print("|   2.Write a file            |")
    print("|   3.Implement Map Reduce    |")
    print("|   4.Exit                    |")
    print(" -----------------------------")

    opt=int(input("Select a Option:"))

    if(opt==1):
        command="python3 Read.py"
        # process=popen(['python3','./Read.py'],stdout=PIPE,stderr=PIPE)
        # stdout,stderr=process.communicate()
        # print(stdout)
        subprocess.run(command,shell=True)
    elif(opt==2):
        command="python3 Write.py"
        # process=popen(['python3','./Write.py'],stdout=PIPE,stderr=PIPE)
        # stdout,stderr=process.communicate()
        # print(stdout)
        subprocess.run(command,shell=True)
    elif(opt==3):
        command="python3 mapreduce3.py"
        # process=popen(['python3','./mapreduce3.py'],stdout=PIPE,stderr=PIPE)
        # stdout,stderr=process.communicate()
        # print(stdout)
        subprocess.run(command,shell=True)
    elif(opt==4):
        exit(0)
    else:
        print("Enter an Appropriate option")
    print("-------------------------------------------------------------------------------------------------------------------------------------------------------------")