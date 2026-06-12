with open("D:\\renko_playback\\backend\\routes\\api.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

# Let's verify line numbers first by printing line content
print(f"Line 1443 (1-indexed): {lines[1442]}")
print(f"Line 1558 (1-indexed): {lines[1557]}")
print(f"Line 1559 (1-indexed): {lines[1558]}")

# Slice out lines 1442 to 1557 (0-indexed, corresponding to 1443 to 1558)
new_lines = lines[:1442] + lines[1558:]

# Find the full cache hit block in the new_lines
cache_hit_idx = -1
for i, line in enumerate(new_lines):
    if "if not missing_pips:" in line:
        cache_hit_idx = i
        break

if cache_hit_idx == -1:
    raise ValueError("Could not find 'if not missing_pips:' in the cleaned lines")

print(f"Found 'if not missing_pips:' at cleaned index {cache_hit_idx+1}")

# Replace the full cache hit block
target = new_lines[cache_hit_idx:cache_hit_idx+10]
print("Target block to replace:")
for idx, line in enumerate(target):
    print(f"  {cache_hit_idx+1+idx}: {line}", end="")

# Let's reconstruct the file content with the correct block
block_to_insert = [
    "    if not missing_pips:\n",
    "        # Full cache hit -- all pips cached\n",
    "        job = job_manager.create_job()\n",
    "        job.status        = \"done\"\n",
    "        job.progress_percent = 100.0\n",
    "        job.result_charts = {k: int(v) for k, v in cached_pip_counts.items()}\n",
    "        job.bricks_built  = job.result_charts\n",
    "        job.engine_used   = \"Cache (PyArrow Parquet)\"\n",
    "        job._base_cache_key = base_cache_key\n",
    "        job._partial_cached_pip_counts = cached_pip_counts\n",
    "        import time as _t\n",
    "        job.completed_at = _t.time()\n",
    "        job.log(f\"All {len(cached_pip_counts)} pips loaded from cache -- no CSV processing needed.\")\n",
    "        return {\"job_id\": job.job_id, \"cache_hit\": True, \"build_mode\": build_mode}\n"
]

# We need to replace the 8 lines of target block with block_to_insert.
# Let's find where the block ends.
block_end_idx = cache_hit_idx
while "return" not in new_lines[block_end_idx]:
    block_end_idx += 1
block_end_idx += 1  # include the return statement line

final_lines = new_lines[:cache_hit_idx] + block_to_insert + new_lines[block_end_idx:]

with open("D:\\renko_playback\\backend\\routes\\api.py", "w", encoding="utf-8") as f:
    f.writelines(final_lines)

print("api.py fixed and updated successfully!")
