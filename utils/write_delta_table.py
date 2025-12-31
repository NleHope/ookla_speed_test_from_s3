import datetime

import numpy as np
import pandas as pd
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from helpers import load_cfg



CFG_FILE = "./utils/config.yaml"

# def main():
#     cfg = load_cfg(CFG_FILE)
#     data_cfg = cfg['data']

#     print("*" * 80)
#     delta_table_fp = f"{data_cfg['folder_path']}"
#     df = pd.DataFrame(
#         {

#                 "event_timestamp": [
#                     np.
#                 ]
#         }
#     )