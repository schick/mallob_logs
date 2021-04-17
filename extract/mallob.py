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

def parse_dominik(jobdir):

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
    
    jobs_list = list(jobs.values())
    jobs_list.sort(key=getID)

    return jobs_list
