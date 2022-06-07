import pandas as pd
from pathlib import Path
from typing import Union


def import_file(filename: Union[str, Path], concat: bool = False, **kwargs):
    chunk_size = kwargs.pop("chunksize", 10 ** 4)
    file_path = Path(filename)
    if not file_path.is_file():
        raise AttributeError(f"{filename} does not exists")
    chunks = []
    with pd.read_csv(filename, chunksize=chunk_size, **kwargs) as reader:
        for chunk in reader:
            chunks.append(chunk)
    if concat:
        return pd.concat(chunks)
    return chunks
