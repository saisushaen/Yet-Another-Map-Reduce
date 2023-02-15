import subprocess
import time
from Read import get_partitions
import multiprocessing
from subprocess import Popen,PIPE
import os

def read_to_one_file(temp_path,output_path):
    print("Process {} started".format(os.getpid()))
    open_partitions=open(temp_path,'r')
    output_open=open(output_path,'a')
    for i in open_partitions.readlines():
        output_open.write(i)
    output_open.close()
    open_partitions.close()
    print("Process {} ended".format(os.getpid()))

def run_sub_process(command):
    subprocess.run(command,shell=True)

def run_sub_process_mapper(partitions_path,mapper_path,tempFiles_path,mapper_arguments):
    print("Process {} started".format(os.getpid()))
#   cat {} | python3 {} > {}
    f=open(tempFiles_path,'w')
    p1 = Popen(["cat",partitions_path], stdout=PIPE)
    p2 = Popen(["python3", mapper_path,mapper_arguments], stdin=p1.stdout, stdout=f)
    p1.stdout.close()
    f.close()
    print("Process {} ended".format(os.getpid()))

def run_sub_process_reducer(reducer_path,tempFiles_path,reducer_arguments,output_file):
    print("Process {} started".format(os.getpid()))

#   cat {} | python3 {} > {}  
    f=open(tempFiles_path,'r')
    f_o=open(output_file,'w')
    p1 = Popen(["cat",tempFiles_path], stdout=PIPE)
    p2 = Popen(["python3", reducer_path,reducer_arguments], stdin=p1.stdout, stdout=f_o)
    p1.stdout.close()
    f.close()
    f_o.close()
    print("Process {} ended".format(os.getpid()))
    
    # output = p2.communicate()[0]
    # print(output)

def hash_func(data,den):
    return data%den



if __name__=='__main__':
    pwd=os.getcwd()
    # partitions_folder=r"/Users/sushaenchittoor/Desktop/BD/Project/Partitions/"
    partitions_folder=pwd+"/Partitions/"
    tempFiles=pwd+"/TempFiles/"
    # tempFiles=r"/Users/sushaenchittoor/Desktop/BD/Project/TempFiles/"
    file=input("Enter the input file name:")
    mapper_path=input("Path Of Mapper:")
    mapper_arguments=input("Arguments for mapper:")
    reducer_path=input("Path Of Reducer:")
    reducer_arguments=input("Arguments for Reducer:")
    output_file=input("Path of Output File:")


    

    #---------- map phase---------------
    # map_start_time=datetime.now()
    print("---------------------Map Phase Started--------------------")
    map_start_time=time.time()
    partitions=get_partitions(file)
    process_list = []
    for i in range(len(partitions)):
        partitions_path=partitions_folder+str(partitions[i])+'/'+file+str(i+1)
        tempFiles_path=tempFiles+str(partitions[i])+'/'+file+str(i+1)
        # command="cat {} | python3 {} > {}".format(partitions_path,mapper_path,tempFiles_path)
        # print(command)
    # print(partitions_path)
    # open_partitions=open(partitions_path,'r')
    # output_open=open(output_path,'a')
    # for i in open_partitions.readlines():
    #     output_open.write(i)
    # for i in range(10):
        p =  multiprocessing.Process(target= run_sub_process_mapper, args = [partitions_path,mapper_path,tempFiles_path,mapper_arguments])

    # p =  multiprocessing.Process(target= read_to_one_file, args = [partitions_path,output_path])
        p.start()
        process_list.append(p)

    for process in process_list:
        process.join()

    # map_end_time=datetime.now()
    map_end_time=time.time()
    print("---------------------Map Phase Completed--------------------")
    print("Time taken for Mapper:",map_end_time-map_start_time)

    #------------Shuffle Phase----------------

    time.sleep(3)
    # shuffle_start_time=time.time()
    # f=open(tempFiles,'r')
    print("---------------------Shuffle Phase Started--------------------")
    shuffle_start_time=time.time()
    # shuffle_output_path="/Users/sushaenchittoor/Desktop/BD/Project/Shuffler_Output/"
    shuffle_output_path=pwd+"/Shuffler_Output/"

    # for i in range(len(partitions)):
    # if(len(partitions)!=1):
    for i in range(len(partitions)):
        tempFiles_path=tempFiles+str(partitions[i])+'/'+file+str(i+1)
        print("--Shuffling ",tempFiles_path,"----------")
        with open(tempFiles_path,'r') as f_shuffle:
            for j in f_shuffle.readlines():
                # d=j.split('\t')
                d=j
                # print(d)
                if(ord(d[0][0])>=48 and ord(d[0][0])<=57):
                    hashed_value=hash_func(int(d[0][0]),len(partitions))
                else:                        
                    hashed_value=hash_func(ord(d[0][0]),len(partitions))
                # print(hashed_value)
                path_shuffle_output=shuffle_output_path+str(partitions[hashed_value])+'/'+file+str(hashed_value+1)
                # print(path_shuffle_output)
                shuffle_partition_open=open(path_shuffle_output,'a')
                shuffle_partition_open.write(j)
                shuffle_partition_open.close()
                # print("done")
        f_shuffle.close()
    # else:
    # tempFiles_path=tempFiles+str(partitions[0])+'/'+file+'1'
    # path_shuffle_output=shuffle_output_path+str(partitions[0])+'/'+file+'1'

    # with open(tempFiles_path,'r') as f_shuffle:
    #     for j in f_shuffle.readlines():
    #         shuffle_partition_open=open(path_shuffle_output,'a')
    #         shuffle_partition_open.write(j)
    #         shuffle_partition_open.close()

    shuffle_end_time=time.time()
    print("---------------------Shuffle Phase Completed--------------------")
    print("Time taken for Shuffle:",shuffle_end_time-shuffle_start_time)
    
    time.sleep(3)
    # -----------Reduce Phase-----------------

    reduce_start_time=time.time()
    print("---------------------Reduce Phase Started--------------------")   
    reducer_output_folder="/Users/sushaenchittoor/Desktop/BD/Project/TempOutput/"
    process_list = []
    for i in range(len(partitions)):
        partitions_path=partitions_folder+str(partitions[i])+'/'+file+str(i+1)
        reducer_input_path=shuffle_output_path+str(partitions[i])+'/'+file+str(i+1)
        otpt_file=reducer_output_folder+str(i+1)


        # command="cat {} | python3 {}".format(tempFiles_path,reducer_path)
        # print(command)
    # print(partitions_path)
    # open_partitions=open(partitions_path,'r')
    # output_open=open(output_path,'a')
    # for i in open_partitions.readlines():
    #     output_open.write(i)
    # for i in range(10):
        p =  multiprocessing.Process(target= run_sub_process_reducer, args = [reducer_path,reducer_input_path,reducer_arguments,otpt_file])
        # p =  multiprocessing.Process(target= run_sub_process, args = [command])

    # p =  multiprocessing.Process(target= read_to_one_file, args = [partitions_path,output_path])
        p.start()
        process_list.append(p)

    for process in process_list:
        process.join()


    output_file=output_file+'ans.txt'
    # for i in range(len(partitions)):
    #     temp_path=reducer_output_folder+str(i+1)
    #     read_to_one_file(temp_path,output_file)
    process_list = []
    for i in range(len(partitions)):
        temp_path=reducer_output_folder+str(i+1)

        p =  multiprocessing.Process(target= read_to_one_file, args = [temp_path,output_file])
        p.start()
        p.join()
        time.sleep(10)
        process_list.append(p)
        
    # for process in process_list:
    #     process.join()

    # map_end_time=datetime.now()
    reduce_end_time=time.time()
    print("---------------------Reduce Phase Completed--------------------")
    print("Time taken for Reducer:",reduce_end_time-reduce_start_time)