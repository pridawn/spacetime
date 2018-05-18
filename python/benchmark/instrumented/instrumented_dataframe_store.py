from spacetime.server.store import dataframe_stores
from rtypes.dataframe.dataframe import dataframe
from rtypes.dataframe.objectless_dataframe import ObjectlessDataframe as dataframe_ol

from benchmark.instrumented.instrumented_dataframe import instrumented_dataframe_wrapper as i_df_wrap
from benchmark.instrumented.instrument import instrument


class instrumented_dataframe_stores(dataframe_stores):
    def start(self):
        self.master_dataframe = i_df_wrap(
            dataframe=(
                dataframe()
                if not self.objectless_server else
                dataframe_ol()))
        self.master_dataframe.start()

    def save_instrumentation_data(self, instrument_filename):
        instrument.write_and_reset(instrument_filename)
