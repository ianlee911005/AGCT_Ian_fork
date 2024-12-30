"""
Dependency injector utilities. Bogus implementation to mimick a proper 
DI provider. This class could be reimplemented if we decide to use
a proper one in the future.
"""


class FileUtil:

    @staticmethod
    def __getitem__(self, service):
        return service


