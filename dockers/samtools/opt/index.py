import subprocess
from subprocess import PIPE
import os
import sys
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

'''Get Reference file'''
ref = utility.get_ref_file()

'''Get AMQP connection'''
amqp_conn = conn.get_amqp_conn()


def get_index_file_name(inputfile):
    return("%s.bai" %(inputfile))


def sort_bam(inputfile, outputfile):
    cmd = "samtools sort %s %s" %(inputfile, outputfile.rsplit(".", 1)[0])
    utility.exec_cmd([cmd])
    return (outputfile)


def create_index(inputfile, outputfile):
    sorted_file = sort_bam(inputfile, outputfile)
    cmd = "samtools index %s && rm %s" %(sorted_file, inputfile)
    utility.exec_cmd([cmd])
    return(get_index_file_name(sorted_file)) 


def create_upload_index(inputfile, outputfile):
    '''Set location of inputfile(bam) download'''
    work_dir = "/tmp"
    destination_file = os.path.join(work_dir, os.path.basename(inputfile))
    '''Download inputfile'''
    utility.gs_cp(inputfile, destination_file)
    '''Set location of sorted output file in local'''
    outputfile_local = os.path.join(work_dir, os.path.basename(outputfile))
    '''Sort and create index'''
    index_file_local = create_index(destination_file, outputfile_local)
    '''Upload sorted file'''
    utility.gs_cp(outputfile_local, outputfile)
    '''Get URI of index file'''
    index_file = os.path.join(os.path.dirname(outputfile), os.path.basename(index_file_local))
    '''Upload index file'''
    utility.gs_cp(index_file_local, index_file)
    '''Delete unsorted bam file from bucket'''
    #bucket.delete(inputfile)


def createIndex(inputfile):

    '''inputfile expects absolute path'''
    print ("Input is: " + inputfile)
    
    '''Handle index file creation(bai files)'''
    file_comp = inputfile.rsplit(".", 1)
    index_file = get_index_file_name(inputfile)
    if not utility.exists(index_file):
        outputfile = "%s_sort.%s" %(file_comp[0], file_comp[1])
        create_upload_index(inputfile, outputfile)
        inputfile = outputfile
        print ("Sorted inputfile is: %s" %(inputfile))

    '''Set output file name with absolute path'''
    outputfile_name = os.path.basename(inputfile) + ".vcf"
    outputfile = os.path.join(output_dir, outputfile_name) 


if process_from_queue:
    '''Channel for consumption'''
    channel_in = amqp_conn.channel()
    channel_in.basic_qos(prefetch_count=1)

    '''Start consuming'''
    while True:
        print ("Waiting for task to arrive.")
        body = conn.consume_from_amqp(channel_in, in_queue)

        if not body:
            break

        createIndex(body)
    
    '''Close consumption channel'''
    conn.close_amqp_channel(channel_in)
else:
    createIndex(input_file)

'''Close AMQP connection'''
conn.close_amqp_channel(amqp_conn)

sys.exit(os.EX_OK)
