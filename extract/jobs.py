from dataclasses import dataclass, fields
import sys
import re
from glob import glob


@dataclass
class Job:
    identifier: int

    start_time: float
    end_time: float = -1.0

    duration: float = -1.0

    result: str = "UNKNOWN"

    start_generate_cubes: float = -1.0
    end_generate_cubes: float = -1.0

    cube_count: int = 0

    generate_duration: float = -1.0

    root_node: str = "NONE"

    number_send_cubes: int = 0
    number_returned_cubes: int = 0

    size_of_used_cube: int = -1
    size_of_added_buffer: int = -1

    # https://stackoverflow.com/questions/60713703/better-way-to-iterate-over-python-dataclass-keys-and-values
    def __str__(self):
        return " ".join([str(getattr(self, field.name)) for field in fields(self)])

# Get identifier as int for sort
def getID(job):
    return job.identifier

# Get the node index of the client
def getClientIndex(jobdir):

    # Get the folder index for sorting
    def getFolderName(path):
        return int(re.search(r'\/(\d+)\/$', path).group(1))

    node_folders = sorted(glob(jobdir + "/*/"), key=getFolderName, reverse=True)

    return getFolderName(node_folders[0])

def parse_mallob(jobdir):

    jobs = dict()

    client_index = str(getClientIndex(jobdir)) 

    # Open client log
    for line in open(jobdir + "/" +  client_index + "/log." + client_index, "r").readlines():
        # Example: 1000.443 31 Introducing job #9 => [26]
        intro_match = re.search(r'^(?P<time>\d+.\d+) \d+ Introducing job #(?P<jobid>\d+) => \[(?P<rootrank>\d+)\]', line)

        if intro_match is not None:
            # Create job
            job = Job(identifier=int(intro_match.group("jobid")), start_time=float(intro_match.group("time")))
            # Add to dict
            jobs[intro_match.group("jobid")] = job

        # Example: 935.274 31 SOLUTION #7 UNSAT rev. 0
        solution_match = re.search(r'^(?P<time>\d+.\d+) \d+ SOLUTION #(?P<jobid>\d+) (?P<result>UNSAT|SAT)', line)

        if solution_match is not None:
            # Add end_time and result to job
            jobs[solution_match.group("jobid")].end_time = float(solution_match.group("time"))
            jobs[solution_match.group("jobid")].duration = jobs[solution_match.group("jobid")].end_time - jobs[solution_match.group("jobid")].start_time
            jobs[solution_match.group("jobid")].result = solution_match.group("result")

        # Example: 1030.619 31 TIMEOUT #5 1000.008545 <= [18]
        timeout_match = re.search(r'^(?P<time>\d+.\d+) \d+ TIMEOUT #(?P<jobid>\d+)', line)

        if timeout_match is not None:
            # Add end_time
            jobs[timeout_match.group("jobid")].end_time = float(timeout_match.group("time"))
            jobs[timeout_match.group("jobid")].duration = jobs[timeout_match.group("jobid")].end_time - jobs[timeout_match.group("jobid")].start_time

    # Parse all job logs (This is cnc specific)
    logfiles = glob(jobdir + "/*/log*#*")
    for logfile in logfiles:

        logfile_match = re.search(r'(?P<node>\d+)#(?P<jobid>\d+)$', logfile)
        node = logfile_match.group("node")
        jobid = logfile_match.group("jobid")

        for line in open(logfile, "r").readlines():

            # Example: 1000.51 <c-1#9:0> Started generating cubes
            start_cube_match = re.search(r'^(?P<time>\d+.\d+) .* Started generating cubes$', line)

            if start_cube_match is not None:
                jobs[jobid].start_generate_cubes = float(start_cube_match.group("time"))
                # Cube generation means we found the root node
                jobs[jobid].root_node = node

            # Example: 1.99917 <c-25#2:0> Cube generation has started
            start_cube_match = re.search(r'^(?P<time>\d+.\d+) .* Cube generation has started$', line)

            if start_cube_match is not None:
                jobs[jobid].start_generate_cubes = float(start_cube_match.group("time"))
                # Cube generation means we found the root node
                jobs[jobid].root_node = node

            # Example: 1022.32 <c-1#9:0> Finished generating cubes
            end_cube_match = re.search(r'^(?P<time>\d+.\d+) .* Finished generating cubes$', line)

            if end_cube_match is not None:
                jobs[jobid].end_generate_cubes = float(end_cube_match.group("time"))
                jobs[jobid].generate_duration = jobs[jobid].end_generate_cubes - jobs[jobid].start_generate_cubes

            # 19.7803 <c-25#2:0> Cube generation has finished
            end_cube_match = re.search(r'^(?P<time>\d+.\d+) .* Cube generation has finished$', line)

            if end_cube_match is not None:
                jobs[jobid].end_generate_cubes = float(end_cube_match.group("time"))
                jobs[jobid].generate_duration = jobs[jobid].end_generate_cubes - jobs[jobid].start_generate_cubes

            # Example: 1024.4 <c-1#9:0> Sent 4 cubes to 1
            send_cubes_match = re.search(r'Sent (?P<count>\d+) cubes to \d+$', line)

            if send_cubes_match is not None:
                jobs[jobid].number_send_cubes += int(send_cubes_match.group("count"))

            # Example: 0.458735 <c-7#1:0> Received 4 failed cubes from 7
            recv_failed_cubes_match = re.search(r'Received (?P<count>\d+) failed cubes from \d+$', line)

            if recv_failed_cubes_match is not None:
                jobs[jobid].number_returned_cubes += int(recv_failed_cubes_match.group("count"))

            # Example: DynamicCubeGeneratorThread created a new dynamic cube
            created_cube_match = re.search(r'DynamicCubeGeneratorThread created a new dynamic cube', line)

            if created_cube_match is not None:
                jobs[jobid].cube_count += 1

            # Example: 85208.8 <c-13#285:0> DynamicCubeGeneratorThread 204: Used cube has size 0
            used_cube_match = re.search(r'Used cube has size (?P<size>\d+)', line)

            if used_cube_match is not None:
                if jobs[jobid].size_of_used_cube == -1:
                    jobs[jobid].size_of_used_cube = int(used_cube_match.group("size"))
                elif int(used_cube_match.group("size")) < jobs[jobid].size_of_used_cube:
                    jobs[jobid].size_of_used_cube = int(used_cube_match.group("size"))

            # Example: 85208.8 <c-13#285:0> DynamicCubeGeneratorThread 204: Size of added buffer from failed assumptions: 0
            used_failed_match = re.search(r'Size of added buffer from failed assumptions: (?P<size>\d+)', line)

            if used_failed_match is not None:
                if jobs[jobid].size_of_added_buffer == -1:
                    jobs[jobid].size_of_added_buffer = int(used_failed_match.group("size"))
                elif int(used_failed_match.group("size")) < jobs[jobid].size_of_added_buffer:
                    jobs[jobid].size_of_added_buffer = int(used_failed_match.group("size"))

    jobs_list = list(jobs.values())
    jobs_list.sort(key=getID)

    return jobs_list
















@dataclass
class Problem:
    identifier: int

    duration: float = -1.0

    result: str = "UNKNOWN"

def get_baseline(baseline_file):

    jobs = []

    for line in open(baseline_file, "r").readlines():
        # Example: Solving of formula /global_data/schreiber/sat_instances/ex009_10.cnf with id 316 took 793 seconds and terminated with Result: s SATISFIABLE
        baseline_match = re.search(r'with id (?P<jobid>\d+) took (?P<time>\d+) seconds .* (?P<result>SATISFIABLE|UNSATISFIABLE|UNKNOWN)$', line)

        if baseline_match is not None:
            jobs.append(Problem(identifier=baseline_match.group("jobid"), duration=float(baseline_match.group("time")), result=baseline_match.group("result")))

    return jobs
