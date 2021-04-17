from dataclasses import dataclass, fields
import sys
import re
from glob import glob


@dataclass
class Generator:
    job: int  # job id
    rank: int  # mpi process rank
    instance: int  # id of this generator instance

    # tree_index: int Removed because this may change after suspension

    times_started: int = 1

    run_time: float = 0.0
    wait_time: float = 0.0
    idle_time: float = 0.0

    processed_cubes: int = 0

    splits: int = 0

    solved: bool = False

    failed_cubes: int = 0

    created_cubes: int = 0
    failed_created_cubes: int = 0

    interruptions: int = 0

    largest_cube: int = 0

    average_time_per_cube: float = -1.0

    # https://stackoverflow.com/questions/60713703/better-way-to-iterate-over-python-dataclass-keys-and-values
    def __str__(self):
        return " ".join([str(getattr(self, field.name)) for field in fields(self)])


@dataclass
class Solver:
    job: int  # job id
    rank: int  # mpi process rank
    instance: int  # id of this generator instance

    # tree_index: int Removed because this may change after suspension

    times_started: int = 1

    run_time: float = 0.0
    wait_time: float = 0.0

    processed_cubes: int = 0

    solves: int = 0

    solved: bool = False

    failed_cubes: int = 0

    interruptions: int = 0

    average_time_per_cube: float = -1.0

    # https://stackoverflow.com/questions/60713703/better-way-to-iterate-over-python-dataclass-keys-and-values
    def __str__(self):
        return " ".join([str(getattr(self, field.name)) for field in fields(self)])


def parse_threads_old(jobdir):
    generators = list()
    solvers = list()

    # Parse all job logs (This is cnc specific)
    logfiles = glob(jobdir + "/*/log*#*")
    for logfile in logfiles:

        logfile_match = re.search(r'(?P<rank>\d+)#(?P<jobid>\d+)$', logfile)
        rank = int(logfile_match.group("rank"))
        job_id = int(logfile_match.group("jobid"))

        # Indexed by instance id
        generators_per_logfile = dict()

        # instance_id -> timestamp of last enter
        generator_enter = dict()
        # instance_id -> timestamp of last expand
        generator_expand = dict()
        # instance_id -> timestamp of last idle
        generator_idle = dict()
        # instance_id -> timestamp of last wait
        generator_wait = dict()

        # instance_id -> [time_per_cube]
        generator_time_per_cube = dict()

        # Indexed by instance id
        solvers_per_logfile = dict()

        # instance_id -> timestamp of last enter
        solver_enter = dict()
        # instance_id -> timestamp of last solve
        solver_solve = dict()
        # instance_id -> timestamp of last wait
        solver_wait = dict()

        # instance_id -> [time_per_cube]
        solver_time_per_cube = dict()

        for line in open(logfile, "r").readlines():

            # --------------------- GENERATOR ---------------------

            # Try to match generator thread line
            generator_line_match = re.search(r'^(?P<time>\d+.\d+) <.*> DynamicCubeGeneratorThread (?P<instance>\d+):', line)

            # Check if we found a generator line
            if generator_line_match is not None:
                time = float(generator_line_match.group("time"))
                instance_id = int(generator_line_match.group("instance"))

                # Create a member in time_per_cube if none exist
                if instance_id not in generator_time_per_cube:
                    generator_time_per_cube[instance_id] = list()

                enter_match = re.search(r'Entering the main loop$', line)

                if enter_match is not None:
                    if instance_id in generators_per_logfile:
                        generators_per_logfile[instance_id].times_started += 1
                    else:
                        generators_per_logfile[instance_id] = Generator(job=job_id, rank=rank, instance=instance_id)

                    # Assert that last leave was found
                    assert instance_id not in generator_enter

                    generator_enter[instance_id] = time

                leave_match = re.search(r'Leaving the main loop$', line)

                if leave_match is not None:
                    # Assert that last enter was found
                    assert instance_id in generator_enter

                    # Increase run time
                    generators_per_logfile[instance_id].run_time += time - generator_enter[instance_id]

                    # Delete last enter timestamp
                    del generator_enter[instance_id]

                start_expand_match = re.search(r'Started expanding a cube', line)

                if start_expand_match is not None:
                    # Assert that last expand was finished
                    assert instance_id not in generator_expand

                    generator_expand[instance_id] = time

                split_literal_match = re.search(r'Found split literal', line)

                if split_literal_match is not None:
                    # Assert that last expand was found
                    assert instance_id in generator_expand

                    generators_per_logfile[instance_id].processed_cubes += 1
                    generators_per_logfile[instance_id].splits += 1

                    generator_time_per_cube[instance_id].append(time - generator_expand[instance_id])

                    # Delete last expand timestamp
                    del generator_expand[instance_id]

                failed_match = re.search(r'The cube failed', line)

                if failed_match is not None:
                    # Assert that last expand was found
                    # assert instance_id in generator_expand

                    # generators_per_logfile[instance_id].processed_cubes += 1
                    generators_per_logfile[instance_id].failed_cubes += 1

                    # generator_time_per_cube[instance_id].append(time - generator_expand[instance_id])

                    # # Delete last expand timestamp
                    # del generator_expand[instance_id]

                solved_match = re.search(r'Found a solution', line)

                if solved_match is not None:
                    # Assert that last expand was found
                    # assert instance_id in generator_expand

                    # generators_per_logfile[instance_id].processed_cubes += 1
                    generators_per_logfile[instance_id].solved = True

                    # generator_time_per_cube[instance_id].append(time - generator_expand[instance_id])

                    # # Delete last expand timestamp
                    # del generator_expand[instance_id]

                interruption_match = re.search(r'Interruption during', line)

                if interruption_match is not None:
                    # Assert that last expand was found
                    assert instance_id in generator_expand

                    generators_per_logfile[instance_id].processed_cubes += 1
                    generators_per_logfile[instance_id].interruptions += 1

                    # Do not log time per cube because of interruption
                    # Delete last expand timestamp
                    del generator_expand[instance_id]

                created_match = re.search(r'created a new dynamic cube with size (?P<cube>\d+)', line)

                if created_match is not None:
                    generators_per_logfile[instance_id].created_cubes += 1

                    if generators_per_logfile[instance_id].largest_cube < int(created_match.group("cube")):
                        generators_per_logfile[instance_id].largest_cube = int(created_match.group("cube"))

                failed_created_match = re.search(r'could not create a new dynamic cube, the expanded cube was pruned$', line)

                if failed_created_match is not None:
                    generators_per_logfile[instance_id].failed_created_cubes += 1

                start_idle_match = re.search(r'waits because there are too many cubes$', line)

                if start_idle_match is not None:
                    # Assert that last idle was finished
                    assert instance_id not in generator_idle

                    generator_idle[instance_id] = time

                end_idle_match = re.search(r'resume(s)? because there are no longer too many cubes$', line)

                if end_idle_match is not None:
                    # Assert that last idle was found
                    assert instance_id in generator_idle

                    generators_per_logfile[instance_id].idle_time += time - generator_idle[instance_id]

                    del generator_idle[instance_id]

                start_wait_match = re.search(r'waits because no cube could be assigned$', line)

                if start_wait_match is not None:
                    # Assert that last wait was finished
                    assert instance_id not in generator_wait

                    generator_wait[instance_id] = time

                end_wait_match = re.search(r'resumes because a cube could be assigned$', line)

                if end_wait_match is not None:
                    # Assert that last wait was found
                    assert instance_id in generator_wait

                    generators_per_logfile[instance_id].wait_time += time - generator_wait[instance_id]

                    del generator_wait[instance_id]

            # --------------------- SOLVER ---------------------

            # Try to match solver thread line
            solver_line_match = re.search(r'^(?P<time>\d+.\d+) <.*> DynamicCubeSolverThread (?P<instance>\d+):', line)

            # Check if we found a solver line
            if solver_line_match is not None:
                time = float(solver_line_match.group("time"))
                instance_id = int(solver_line_match.group("instance"))

                # Create a member in solver_time_per_cube if none exist
                if instance_id not in solver_time_per_cube:
                    solver_time_per_cube[instance_id] = list()

                enter_match = re.search(r'Entering the main loop$', line)

                if enter_match is not None:
                    if instance_id in solvers_per_logfile:
                        solvers_per_logfile[instance_id].times_started += 1
                    else:
                        solvers_per_logfile[instance_id] = Solver(job=job_id, rank=rank, instance=instance_id)

                    # Assert that last leave was found
                    assert instance_id not in solver_enter

                    solver_enter[instance_id] = time

                leave_match = re.search(r'Leaving the main loop$', line)

                if leave_match is not None:
                    # Assert that last enter was found
                    assert instance_id in solver_enter

                    # Increase run time
                    solvers_per_logfile[instance_id].run_time += time - solver_enter[instance_id]

                    # Delete last enter timestamp
                    del solver_enter[instance_id]

                start_solve_match = re.search(r'Started solving a cube', line)

                if start_solve_match is not None:
                    # Assert that last expand was finished
                    assert instance_id not in solver_solve

                    solver_solve[instance_id] = time

                failed_match = re.search(r'The cube failed', line)

                if failed_match is not None:
                    # Assert that last expand was found
                    assert instance_id in solver_solve

                    solvers_per_logfile[instance_id].processed_cubes += 1
                    solvers_per_logfile[instance_id].failed_cubes += 1

                    solver_time_per_cube[instance_id].append(time - solver_solve[instance_id])

                    # Delete last expand timestamp
                    del solver_solve[instance_id]

                solved_match = re.search(r'Found a solution', line)

                if solved_match is not None:
                    # Assert that last expand was found
                    assert instance_id in solver_solve

                    solvers_per_logfile[instance_id].processed_cubes += 1
                    solvers_per_logfile[instance_id].solved = True

                    solver_time_per_cube[instance_id].append(time - solver_solve[instance_id])

                    # Delete last expand timestamp
                    del solver_solve[instance_id]

                interruption_match = re.search(r'Interruption during', line)

                if interruption_match is not None:
                    # Assert that last expand was found
                    assert instance_id in solver_solve

                    solvers_per_logfile[instance_id].processed_cubes += 1
                    solvers_per_logfile[instance_id].interruptions += 1

                    # Do not log time per cube because of interruption
                    # Delete last expand timestamp
                    del solver_solve[instance_id]

                start_wait_match = re.search(r'waits because no cube could be assigned$', line)

                if start_wait_match is not None:
                    # Assert that last wait was finished
                    assert instance_id not in solver_wait

                    solver_wait[instance_id] = time

                end_wait_match = re.search(r'resumes because a cube could be assigned$', line)

                if end_wait_match is not None:
                    # Assert that last wait was found
                    assert instance_id in solver_wait

                    solvers_per_logfile[instance_id].wait_time += time - solver_wait[instance_id]

                    del solver_wait[instance_id]

            # Wenn hier noch welche laufen liegt das an dem alten Verhalten zu returnen und nicht zu breaken, wenn gelöst wurde
            # Hier Hack
            joined_line_match = re.search(r'^(?P<time>\d+.\d+) .* Joined dynamic cube lib$', line)

            if joined_line_match is not None:
                time = float(joined_line_match.group("time"))

                for key in generator_enter.keys():
                    # Increase run time
                    generators_per_logfile[key].run_time += time - generator_enter[key]

                generator_enter.clear()

                for key in solver_enter.keys():
                    # Increase run time
                    solvers_per_logfile[key].run_time += time - solver_enter[key]

                solver_enter.clear()
            

            # Check for last line
            last_line_match = re.search(r'Destructing logger$', line)

            if last_line_match is not None:
                # Assert that all dicts are empty
                assert not generator_enter
                assert not generator_expand
                assert not generator_idle
                assert not generator_wait

                assert not solver_enter
                assert not solver_solve
                assert not solver_wait

                # Calculate average time per cube
                for instance_id in generator_time_per_cube.keys():
                    if generator_time_per_cube[instance_id]:
                        generators_per_logfile[instance_id].average_time_per_cube = sum(generator_time_per_cube[instance_id]) / len(generator_time_per_cube[instance_id])

                for instance_id in solver_time_per_cube.keys():
                    if solver_time_per_cube[instance_id]:
                        solvers_per_logfile[instance_id].average_time_per_cube = sum(solver_time_per_cube[instance_id]) / len(solver_time_per_cube[instance_id])

                # Reset time per cube
                generator_time_per_cube = dict()

                solver_time_per_cube = dict()

                # Add generators and solvers of this log file to the global lists
                for generator in generators_per_logfile.values():
                    generators.append(generator)

                for solver in solvers_per_logfile.values():
                    solvers.append(solver)

    return generators, solvers
