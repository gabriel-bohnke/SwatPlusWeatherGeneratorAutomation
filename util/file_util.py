"""
Author........... Gabriel Böhnke
University....... UCLouvain, Faculty of bioscience engineering
Email............ gabriel.bohnke@student.uclouvain.be

Description...... file util functions
Version.......... 1.00
Last changed on.. 02.05.2022
"""

import os
import shutil

# Delete all files in a directory in Python
# https://www.techiedelight.com/delete-all-files-directory-python/
def delete_complete_directory(directory):
    if os.path.exists(directory):
        shutil.rmtree(directory)
        print(directory + ' deleted')