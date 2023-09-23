import os
from .. import task1
#from task1 import all_time_best_100movies


def test_all_time_best_100movies():
    test_df = task1.all_time_best_100movies()
    assert test_df.count() == 100
