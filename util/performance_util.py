"""
Author........... Gabriel Böhnke
University....... UCLouvain, Faculty of bioscience engineering
Email............ gabriel.bohnke@student.uclouvain.be

Description...... performance util functions
Version.......... 1.00
Last changed on.. 02.05.2022
"""

import time
from datetime import timedelta


# How do I get time of a Python program's execution?
# https://stackoverflow.com/questions/1557571/how-do-i-get-time-of-a-python-programs-execution
def start_time_measure(message=None):
    if message:
        print(message)
    return time.monotonic()


def end_time_measure(start_time, print_prefix=None):
    end_time = time.monotonic()
    if print_prefix:
        print(print_prefix + str((timedelta(seconds=end_time - start_time))).split('.')[0])  # remove µs
    return end_time
