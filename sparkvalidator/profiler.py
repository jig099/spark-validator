
### Profiler APIs ###
##https://docs.python.org/3/library/profile.html##
# 1. profile
import cProfile

"""
Profiler will take in function name and gathers profiling statistics from the execution
"""
def profile(function_name):
    cProfile.run(function_name)
