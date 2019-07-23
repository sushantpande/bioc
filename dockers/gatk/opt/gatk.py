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


def runGATK(inputfile):

    '''inputfile expects absolute path'''
    print ("Input is: " + inputfile)
    '''Set output file name with absolute path'''
    outputfile_name = os.path.basename(inputfile) + ".vcf"
    outputfile = os.path.join(output_dir, outputfile_name) 

    '''Exec GATK command - HaplotypeCaller'''
    cmd = "gatk HaplotypeCaller -R " + ref + " -I " + inputfile + " -O " +  outputfile
    print ("Executing: " + cmd)  
    completedProc = subprocess.run([cmd, "/dev/null"], shell=True, stdout=PIPE, stderr=PIPE)

    print ("GATK output: %s" %(completedProc.stdout))
    print ("GATK error: %s" %(completedProc.stderr))


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

        runGATK(body)
    
    '''Close consumption channel'''
    conn.close_amqp_channel(channel_in)
else:
    runGATK(input_file)

'''Close AMQP connection'''
conn.close_amqp_channel(amqp_conn)

sys.exit(os.EX_OK)
