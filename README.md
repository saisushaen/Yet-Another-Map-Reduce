# Yet-Another-Map-Reduce
Implemented the core components of Hadoop’s Map Reduce Framework as a part of this project.


To run the project YAMR, make sure you have Python3 installed on your system. Then, navigate to the directory where you have saved the project files, and open a terminal or command prompt.

1. First, create the necessary folders in the project directory. These include:
   - Partitions (1 to 9 folders within this folder)
   - TempFiles
   - TempOutput
   - Shuffler_Output

2. Next, run the app.py file using the command: python3 app.py

3. The program will prompt you to enter the input file name and the number of partitions. Make sure the input file is located in the same directory as the project files, and enter the number of partitions you want to use.

4. The program will then begin the map-reduce process, which includes:
   - Reading the input file and dividing it into partitions
   - Running the map function on each partition
   - Combining the output from each partition in the shuffler
   - Running the reduce function on the combined output
  
5. The final output will be written to the TempOutput folder, and the intermediate files will be stored in the TempFiles and Shuffler_Output folders.

6. The program will display a message when it is finished. The final output can be found in the TempOutput folder.

Please note that the program may take some time to run depending on the size of the input file and the number of partitions used.
