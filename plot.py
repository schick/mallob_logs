import re
import sys
import matplotlib
matplotlib.use('Qt5Agg') 
import matplotlib.pyplot as plt

timestamps = []
memory_per_timestamp = []

# Collect data
for line in open(sys.argv[1], "r").readlines():
    line = line.replace("\n", "")
    accmem_match = re.search(r'(accmem=[0-9]+\.[0-9]+)', line)
    
    if not accmem_match:
        continue
    
    memory_per_timestamp.append(float(accmem_match.group(1)[7:]))

    timestamp_match = re.search(r'(^[0-9]+\.[0-9]+)', line)

    timestamps.append(float(timestamp_match.group(1)))
    
# Plot data
plt.figure(figsize=(4.5,3.5))

plt.plot(timestamps, memory_per_timestamp)

# Show data
plt.legend()
plt.title("\\textit{mallob}: Memory consumption over time")
plt.xlabel("Elapsed time / s")
#plt.xlim(min_time, max_time)
plt.ylabel("Memory consumption (GB)")
plt.ylim(0, None)
plt.tight_layout()
plt.show()
#plt.savefig("out.pdf")
