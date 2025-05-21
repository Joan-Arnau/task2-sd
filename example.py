# Welcome to PyRun!

# To help you get started, we have included a small example
# showcasing how to use lithops.

# To install more packages, please edit the environment.yml
# file found in the .pyrun directory.


import lithops
import time


def my_map_function(id, x):
    print(f"I'm activation number {id}")
    time.sleep(5)
    return x + 7


if __name__ == "__main__":
    iterdata = [10, 11, 12, 13]
    fexec = lithops.FunctionExecutor()
    fexec.map(my_map_function, range(2))
    fexec.map(my_map_function, iterdata)
    print(fexec.get_result())