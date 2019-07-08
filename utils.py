from subprocess import run, PIPE, Popen

def load_spark_conf(file_path):
    spark_conf = {}
    with open(file_path, 'r') as conf_fh:
        for conf in conf_fh:
            data = conf.split()
            if data:
                spark_conf.update({data[0]:data[1]})
    return (spark_conf)


def exec_cmd(cmd):
    cmd.append("/dev/null")
    completed_proc = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    return (completed_proc)
