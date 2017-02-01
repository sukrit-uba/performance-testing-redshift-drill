# performance-testing-redshift-drill
performance_test_drill.py launches multiple parallel queries to drill. For it to run drill has to be started locally first.

performance_testing_redshift.py launches multiple processes using python's multiprocessing module to launch parallel queries to redshift. Each process is a seperate db connection to redshift using python's psycopg2 module. 
