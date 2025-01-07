"""
Dependency injector utilities. Bogus implementation to mimick a proper 
DI provider. This class could be reimplemented if we decide to use
a proper one in the future.
"""


def new_line(out, num_nls: int = 1):
    nls = "\n" * num_nls
    out.write(nls)
