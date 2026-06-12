import sys
from pathlib import Path
backend_dir = Path("D:/renko_playback/backend")
sys.path.insert(0, str(backend_dir))

from services.job_manager import job_manager

job_id = "1597ec20-193d-4c83-886d-73fcf39cc769"
job = job_manager.get_job(job_id)
if job:
    print("Job found in memory!")
    print("status:", job.status)
    print("result_charts:", job.result_charts)
    print("bricks_built:", job.bricks_built)
    print("engine_used:", job.engine_used)
    print("_base_cache_key:", getattr(job, "_base_cache_key", None))
    print("_partial_cached_pip_counts:", getattr(job, "_partial_cached_pip_counts", None))
    print("Logs:")
    for l in job.logs:
        print("  ", l)
else:
    print("Job not found in memory!")
