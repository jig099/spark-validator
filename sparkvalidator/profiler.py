
'''
comment example:
def test_function (p1,p2,p3):
   """
   test_function does blah blah blah.

   :param p1: describe about parameter p1
   :param p2: describe about parameter p2
   :param p3: describe about parameter p3
   :return: describe what it returns
'''
### Profiler APIs ###
##https://docs.python.org/3/library/profile.html##
##pyspark profiler source code: https://spark.apache.org/docs/2.3.1/api/python/_modules/pyspark/profiler.html##
# run the cprofile and analyze the result


def profiler(user_file):
    cProfile.run('re.compile("foo|bar")')
