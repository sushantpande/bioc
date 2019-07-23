import subprocess
from subprocess import PIPE
import os
import sys
import split
import utility
import conn


process_from_queue = False

'''Get input file'''
in_queue = None
bucket = None
input_file = utility.get_input_file()
work_dir = utility.get_work_dir()

if not input_file:
    print ("No input file for Job, looking for input queue")

    '''Get input queue'''
    in_queue_str = utility.get_env_param('IN_QUEUE')
    in_queue_list = in_queue_str.split()

    if not in_queue_list:
        print ("Found no in_queue for job")
        sys.exit(os.EX_NOTFOUND)
    else:
        in_queue = in_queue_list[0]
        process_from_queue = True
    

'''Get output dir'''
output_dir = utility.get_output_dir() 

'''Get AMQP connection'''
amqp_conn = conn.get_amqp_conn()


def runSplit(inputfile):
    '''inputfile expects absolute path'''
    print ("Input is: " + inputfile)
    '''Download Inputfile from Bucket''' 
    work_dir = "/tmp"
    destination_file = os.path.join(work_dir, os.path.basename(inputfile))
    utility.gs_cp(inputfile, destination_file)
    '''Download Index file from Bucket''' 
    inputfile_index = "%s.bai" %(inputfile)
    destination_file_index = "%s.bai" %(destination_file)
    utility.gs_cp(inputfile_index, destination_file_index)
    '''Set location of output dir in local'''
    split_file_list = split.split_by_chr(destination_file, work_dir)
    '''upload_split_files to Bucket'''
    for file in split_file_list:
        utility.gs_cp(file, output_dir)
    

if process_from_queue:
    '''Channel for consumption'''
    in_channel = amqp_conn.channel()
    in_channel.basic_qos(prefetch_count=1)

    '''Start consuming'''
    while True:
        print ("Waiting for task to arrive.")
        body = conn.consume_from_amqp(in_channel, in_queue)

        if not body:
            break

        runSplit(body)
    
    '''Close consumption channel'''
    conn.close_amqp_channel(in_channel)
else:
    runGATK(input_file)

'''Close AMQP connection'''
conn.close_amqp_channel(amqp_conn)

sys.exit(os.EX_OK)
