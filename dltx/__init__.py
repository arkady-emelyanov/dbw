class DltMock:

    def table(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:
        :return:
        """

        def inner_func(*a, **k):
            print("table:", self, args, kwargs, a, k)

        return inner_func

    def create_target_table(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:
        :return:
        """

        def inner_func(*a, **k):
            print("create_target_table:", self, args, kwargs, a, k)

        return inner_func

    def read(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:
        :return:
        """

        def inner_func(*a, **k):
            print("read:", self, args, kwargs, a, k)

        return inner_func

    def read_stream(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:
        :return:
        """

        def inner_func(*a, **k):
            print("read_stream:", self, args, kwargs, a, k)

        return inner_func


try:
    import dlt
except ImportError:
    import sys

    sys.modules["dlt"] = DltMock()


class DltJob:
    pass
