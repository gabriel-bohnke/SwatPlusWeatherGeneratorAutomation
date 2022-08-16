"""
Author........... Gabriel Böhnke
University....... UCLouvain, Faculty of bioscience engineering
Email............ gabriel.bohnke@student.uclouvain.be

Description...... reset folders
Version.......... 1.00
Last changed on.. 02.05.2022
"""

from util.file_util import delete_complete_directory


def main():

    delete_complete_directory('SWAT_INPUT_DATA')
    # delete_complete_directory('GEE_RAW_DATA')  # <-- very expensive data fetch: are your sure?

if __name__ == '__main__':

    main()
