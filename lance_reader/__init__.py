import lance


class LanceDataFrameIO:
    @staticmethod
    def read(uri, version=None, asof=None):
        """
        Load Lance file to a pandas DataFrame.

        Ref: https://eto-ai.github.io/lance/api/python/lance.html#lance.dataset
        """
        return lance.dataset(uri, version, asof).to_table().to_pandas()

    def write(self, uri, schema=None, mode='create', max_rows_per_file=1048576, max_rows_per_group=1024):
        """
        Export dataframe to Lance file.

        Ref: https://eto-ai.github.io/lance/api/python/lance.html#lance.write_dataset
        """
        lance.write_dataset(self, uri, schema, mode, max_rows_per_file, max_rows_per_group)
