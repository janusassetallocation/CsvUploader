import yaml
import pandas as pd

HEADER_END_MAGIC = '-' * 40


def _calc_offsets(stream):
    stream.seek(0)
    yaml_end_pos, csv_start_pos, metadata = 0, 0, None
    pos = 0
    for line in iter(stream.readline, ''):
        pos, prev_pos = stream.tell(), pos
        if line == HEADER_END_MAGIC + '\n':
            yaml_end_pos = prev_pos
            csv_start_pos = pos
            break
    return yaml_end_pos, csv_start_pos


class HeaderCsv(object):
    def __init__(self, metadata, df):
        self.stream = None
        self._yaml_end_pos = None
        self._csv_start_pos = None
        self.metadata = metadata
        self._df = df
        self._serialize_df = False

    @property
    def yaml_end_pos(self):
        if self._yaml_end_pos:
            return self._yaml_end_pos
        if self.stream is None:
            return None
        self._yaml_end_pos, self._csv_start_pos = _calc_offsets(self.stream)
        return self._yaml_end_pos

    @property
    def csv_start_pos(self):
        if self._csv_start_pos:
            return self._csv_start_pos
        if self.stream is None:
            return None
        self._yaml_end_pos, self._csv_start_pos = _calc_offsets(self.stream)
        return self._csv_start_pos

    @property
    def df(self):
        if self._df is not None:
            return self._df
        if self.stream is not None:
            self.stream.seek(self.csv_start_pos)
            self._df = pd.read_csv(self.stream, index_col=0, parse_dates=True)
            return self._df
        return None

    @df.setter
    def df(self, value):
        self._df = value
        self._serialize_df = True

    @staticmethod
    def load(stream):
        result = HeaderCsv(None, None)
        result.stream = stream
        if result.csv_start_pos > 0:
            stream.seek(0)
            metadata_bytes = stream.read(result.yaml_end_pos - 1)
            result.metadata = yaml.load(metadata_bytes)
        return result

    def dump(self, stream):
        yaml.dump(self.metadata, stream)
        stream.write(HEADER_END_MAGIC + '\n')
        if self._serialize_df:
            self.stream.seek(self.csv_start_pos)
            while True:
                chunk = self.stream.read(8192)
                if chunk:
                    stream.write(chunk)
                else:
                    break
        else:
            self._df.to_csv(stream)
