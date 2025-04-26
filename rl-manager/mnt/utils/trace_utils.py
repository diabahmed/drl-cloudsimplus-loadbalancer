import pandas as pd


def trace_to_csv(jobs: pd.DataFrame, filename="data.csv"):
    jobs.to_csv(filename)


def as_cloudlet_descriptor_dict(job_id, submit_time, mi, allocated_cores):
    return {
        "jobId": job_id,
        "submissionDelay": submit_time,
        "mi": mi,
        "cores": allocated_cores,
    }


def csv_to_cloudlet_descriptor(filename="data.csv"):
    jobs = []
    df = pd.read_csv(filename, sep=",")
    for i in df.index:
        cloudlet = as_cloudlet_descriptor_dict(
            int(df.job_id[i]),
            int(df.arrival_time[i]),
            int(df.mi[i]),
            int(df.allocated_cores[i]),
        )
        jobs.append(cloudlet)
    return jobs


def swf_to_cloudlet_descriptor(
    filename, jobs_to_read=None, relative_submission_delay=True
):
    """
    Format as specified in
    http://www.cs.huji.ac.il/labs/parallel/workload/swf.html
    """
    jobs = []
    # mips = 1250
    # controls how long jobs run
    mips = 10
    previous_submit_time = None
    with open(filename, "r") as f:
        jobs_read = 0
        for line in f.readlines():
            if line.startswith(";"):
                continue
            if jobs_to_read is not None and jobs_read == jobs_to_read:
                return jobs

            line_stripped = line.strip()
            line_splitted = line_stripped.split()

            job_id = int(line_splitted[0])
            submit_time = int(line_splitted[1])
            run_time = int(line_splitted[3])
            allocated_cores = int(line_splitted[4])
            status = int(line_splitted[10])
            mi = run_time * mips * allocated_cores

            if status != 0 or run_time <= 0 or allocated_cores <= 0:
                continue

            if relative_submission_delay:
                if previous_submit_time is None:
                    # The first job's relative submission time is 0
                    previous_submit_time = submit_time
                    submit_time = 0
                else:
                    # Update submit_time to be relative to the previous job
                    original_submit_time = submit_time
                    submit_time -= previous_submit_time
                    previous_submit_time = original_submit_time

            cloudlet = as_cloudlet_descriptor_dict(
                job_id, submit_time, mi, allocated_cores
            )
            jobs.append(cloudlet)
            jobs_read += 1
    return jobs
