import multiprocessing
import os
import time

# f=open("/Users/sushaenchittoor/Desktop/BD/Project/logs","r")

# output_path_local=r"/Users/sushaenchittoor/Desktop/BD/Project/Data/output"
# file="test"


def read_to_one_file(partitions_path,output_path):
    # print("Process {} started,{}".format(partitions_path,os.getpid()))
    print("Process {} started".format(os.getpid()))
    open_partitions=open(partitions_path,'r')
    output_open=open(output_path,'a')
    for i in open_partitions.readlines():
        output_open.write(i)
    output_open.close()
    open_partitions.close()
    print("Process {} ended".format(os.getpid()))

def get_partitions(filename):
    partitions_str=[]
    pwd=os.getcwd()
    # f=open(r"/Users/sushaenchittoor/Desktop/BD/Project/logs","r")
    f=open(pwd+"/logs","r")
    for i in f.readlines():
        i=i.split('\t')
    # print(i,type(i[1]))
        if i[0]==filename:
            partitions_str=i[1]
    partitions=[]
    for i in partitions_str:
        if i!='[' and i!=']' and i!=' ' and i!=',' and i!='\n':
            partitions.append(int(i))
    return partitions

if __name__ == '__main__':

#     for i in f.readlines():
#         i=i.split('\t')
#     # print(i,type(i[1]))
#         if i[0]==file:
#             partitions_str=i[1]
#     partitions=[]
# # print(partitions_str)
#     for i in partitions_str:
#         if i!='[' and i!=']' and i!=' ' and i!=',' and i!='\n':
#             partitions.append(int(i))
# # partitions_2=list(partitions)

# print(partitions)
    pwd=os.getcwd()
    f=open(pwd+"/logs","r")
    partitions_folder=pwd+"/Partitions/"
    file=input("Enter the name of the File:")
    output_path_local=input("Enter the Path Of Output:")
    partitions=get_partitions(file)
    process_list = []
    for i in range(len(partitions)):
        partitions_path=partitions_folder+str(partitions[i])+'/'+file+str(i+1)
    # print(partitions_path)
    # open_partitions=open(partitions_path,'r')
    # output_open=open(output_path,'a')
    # for i in open_partitions.readlines():
    #     output_open.write(i)
    # for i in range(10):
        p =  multiprocessing.Process(target= read_to_one_file, args = [partitions_path,output_path_local])
        p.start()
        process_list.append(p)
        time.sleep(3)

    for process in process_list:
        process.join()

# for i in range(10):
#     p =  multiprocessing.Process(target= sleepy_man, args = [2])
#     p.start()
#     process_list.append(p)

# for process in process_list:
#     process.join()