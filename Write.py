import math
import random
import multiprocessing
import os

# partitions_folder=r"/Users/sushaenchittoor/Desktop/BD/Project/Partitions/"
pwd=os.getcwd()
partitions_folder=pwd+"/Partitions/"

# def add_to_file(path,file_name,partition,data):
#     path=path+str(partition)+'/'+file_name+str(partition)
#     f=open(path,"a")
#     for i in data:
#         f.write(i)
#     f.close()

def add_to_file(file_name,partition,data,partition_index):
    print("Process {} Started".format(os.getpid()))
    path=partitions_folder+str(partition)+'/'+file_name+str(partition_index)
    # print(path)
    f=open(path,"a")
    for i in data:
        f.write(i)
    f.close()
    print("Process {} Ended".format(os.getpid()))

if __name__=='__main__':
    file_path=input("Enter path of the file:")
    # file_path=r"/Users/sushaenchittoor/Desktop/BD/Project/Data/tech.txt"
    file_name=file_path.split('/')[-1]
    no_of_lines=0

    fp=open(file_path, 'r')
    for i in fp.readlines():
        no_of_lines+=1
    
    fp=open(file_path, 'r')
    print(no_of_lines)
    input_partitions=int(input("Enter Number of partitions(btwn 1 to 9):"))
    # max_partition_size=8000

    l=[]
    c=0
    # no_of_partitions=math.ceil(no_of_lines/max_partition_size)
    no_of_partitions=input_partitions
    max_partition_size=math.ceil(no_of_lines/input_partitions)

    # getting partitions randomly
    partitions=[]
    i=0
    while(i<no_of_partitions):
        s=random.randrange(1,10)
        if s not in partitions:
            partitions.append(s)
            i+=1
    print(partitions)

    i=0

    l_outer=[]
    l_inner=[]
    for j in fp.readlines():
        if(len(l_inner)<max_partition_size):
            l_inner.append(j)
            c+=1
        else:
            l_outer.append(l_inner)
            l_inner=[]
            l_inner.append(j)
    l_outer.append(l_inner)

# for i in range(len(l_outer)):
#     add_to_file("/Users/sushaenchittoor/Desktop/BD/Project/Partitions/",file_name,partitions[i],l_outer[i])

    process_list = []
    for i in range(len(l_outer)):
        # add_to_file("/Users/sushaenchittoor/Desktop/BD/Project/Partitions/",file_name,partitions[i],l_outer[i])
        # add_to_file(file_name,partitions[i],l_outer[i])
        p =  multiprocessing.Process(target= add_to_file, args = [file_name,partitions[i],l_outer[i],i+1])
        p.start()
        process_list.append(p)

    for process in process_list:
        process.join()

    log_file=open(pwd+"/logs",'a')
    log_file.write(file_name+'\t'+str(partitions)+'\n')
    log_file.close()