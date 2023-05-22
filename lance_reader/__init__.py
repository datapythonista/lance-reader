"""
Implementation of the dataframe I/O connector API for LanceDB.

The idea is that if pandas (and hopefully other libraries later)
find an entrypoint pointing to this class in the `dataframe.io` group,
it will automatically create `pandas.read_lance()`, `DataFrame.to_lance()`.

This is implemented in this repo for illustration. In practice, it makes
much more sense that this is directly implemented in the LanceDB project.
"""
import lance


class LanceDataFrameIO:
    @staticmethod
    def pyarrow_reader(uri, version=None, asof=None):
        """
        Load Lance file to a pandas DataFrame.

        Ref: https://eto-ai.github.io/lance/api/python/lance.html#lance.dataset
        """
        return lance.dataset(uri, version, asof).to_table()

    def pyarrow_writer(self, uri, schema=None, mode='create', max_rows_per_file=1048576, max_rows_per_group=1024):
        """
        Export dataframe to Lance file.

        Ref: https://eto-ai.github.io/lance/api/python/lance.html#lance.write_dataset
        """
        lance.write_dataset(self, uri, schema, mode, max_rows_per_file, max_rows_per_group)

    # The next two methods are for illustration, but they don't really need to be implemented as pandas
    # will fallback to `pyarrow_reader` and `pyarrow_writer` if they are not found

    @staticmethod
    def pandas_reader(uri, version=None, asof=None):
        return LanceDataFrameIO.pyarrow_reader(uri, version, asof).to_pandas()

    def pandas_writer(self, uri, schema=None, mode='create', max_rows_per_file=1048576, max_rows_per_group=1024):
        self.pyarrow_writer(uri, schema, mode, max_rows_per_file, max_rows_per_group)
