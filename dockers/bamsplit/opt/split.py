import os
import pysam
import time
import glob
import utility


def split_by_file_count(input_file, output_dir, count=10):
    cmd = "samtools view " + input_file + " | wc -l"
    print ("cmd: %s" %cmd)
    completed_proc = exec_cmd([cmd])
    print ("stdout: %s" %(completed_proc.stdout))
    print ("stderr: %s" %(completed_proc.stderr))

    per_file_count = int(int(completed_proc.stdout)/count)

    if per_file_count < 1:
        print("Cannot split file: Reduce split size")
        lc = int(completed_proc.stdout)
    else:
        lc = per_file_count

    print ("Lines per file would be ~ %s" %(lc))
    header_file = os.path.join(output_dir, "header")
    glob_output_dir = os.path.join(output_dir, "*")

    cmd = "samtools view -H " + input_file + " > " + header_file
    print ("cmd: %s" %cmd)
    print ("stdout: %s" %(completed_proc.stdout))
    print ("stderr: %s" %(completed_proc.stderr))

    split_prefix = os.path.join(output_dir,"bwaoutput")

    cmd = "samtools view " + input_file + " | split - " + split_prefix + " -l " + str(lc) + " --filter='cat " + header_file + " - | samtools view -b - > $FILE.bam' && rm " + header_file     
    print ("cmd: %s" %cmd)
    print ("stdout: %s" %(completed_proc.stdout))
    print ("stderr: %s" %(completed_proc.stderr))

    file_list = glob.glob(glob_output_dir)

    for file in file_list:
        cmd = "samtools index " + file
        print ("cmd: %s" %cmd)
        completed_proc = exec_cmd([cmd])
        print ("stdout: %s" %(completed_proc.stdout))
        print ("stderr: %s" %(completed_proc.stderr))

    return (file_list)


def split_by_chr(input_file, output_dir):    
    file_list = []
    cmd = "samtools view -H" + " " + input_file + " | cut -f2 | grep '^SN:' | sed s'/SN://'"
    completed_proc = utility.exec_cmd([cmd])
    print ("stdout: %s" %(completed_proc.stdout))
    print ("stderr: %s" %(completed_proc.stderr))

    samfile = pysam.AlignmentFile(input_file, "rb")

    for chr in (completed_proc.stdout.decode('utf-8').split("\n")):
        if not chr:
            continue
        print ("Fetching chr: " + chr)
        reads = samfile.fetch(chr)
        outfile = None

        if reads:
            filename = chr + "_split.bam"
            filepath = os.path.join(output_dir, filename)
            outfile = pysam.AlignmentFile(filepath, "wb", template=samfile)

            for read in reads:
                outfile.write(read)
            outfile.close()

            pysam.index(filepath)
            file_list.append(filepath)
            print(" [x] Sent %r" % filepath)

        samfile.close()
        return (file_list)
