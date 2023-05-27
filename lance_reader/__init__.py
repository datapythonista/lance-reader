"""
Implementation of the dataframe I/O connector API for LanceDB.

The idea is that if pandas (and hopefully other libraries later)
find an entrypoint pointing to this class in the `dataframe.io` group,
it will automatically create `pandas.read_lance()`, `DataFrame.to_lance()`
and `Series.to_lance()` functions and methods. The name `lance` in `read_lance`
and `to_lance` is obtained from the name of the entrypoint defined in this
project, not decided by pandas itself.

This is implemented in this repo for illustration. In practice, it makes
much more sense that this is directly implemented in the LanceDB project.
"""
import pyarrow.interchange
import lance


class LanceDataFrameIO:
    @staticmethod
    def reader(uri,
               version=None,
               asof=None,
               columns=None,
               filter_=None,
               nearest=None):
        """
        Load the LanceDB dataset, and return it as a PyArrow Table. The format
        can be anything that implements the dataframe interchange protocol.

        References
        ----------
        - https://eto-ai.github.io/lance/api/python/lance.html#lance.dataset
        - https://data-apis.org/dataframe-protocol/latest/API.html
        """
        return (lance.dataset(uri,
                              version,
                              asof)
                     .to_table(columns,
                               filter_,
                               nearest))

    def writer(self,
               uri,
               schema=None,
               mode='create',
               max_rows_per_file=1048576,
               max_rows_per_group=1024):
        """
        Write object to a LanceDB file.

        The received object must implement the dataframe interchange protocol.

        Ref: https://eto-ai.github.io/lance/api/python/lance.html#lance.write_dataset
        """
        lance.write_dataset(data_obj=pyarrow.interchange.from_dataframe(self),
                            uri=uri,
                            schema=schema,
                            mode=mode,
                            max_rows_per_file=max_rows_per_file,
                            max_rows_per_group=max_rows_per_group)
