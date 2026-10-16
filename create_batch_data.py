from transaction_publisher import generateTransactionEvent

import pandas as pd

if __name__ == "__main__":
    data_list = [generateTransactionEvent() for _ in range(10000)]
    df = pd.DataFrame(data_list)
    output_filename = "batch_data.parquet"
    df.to_parquet(output_filename, engine='pyarrow', compression='snappy')