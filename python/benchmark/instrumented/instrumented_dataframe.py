from benchmark.instrumented.instrument import instrument
from rtypes.dataframe.dataframe_threading import dataframe_wrapper


class instrumented_dataframe_wrapper(dataframe_wrapper):

    @instrument("server.dataframe.shutdown")
    def shutdown(self):
        return super(instrumented_dataframe_wrapper, self).shutdown()

    @instrument("server.dataframe.process_get_req")
    def process_get_req(self, *args, **kwargs):
        return super(instrumented_dataframe_wrapper, self).process_get_req(
            *args, **kwargs)

    @instrument("server.dataframe.process_apply_req")
    def process_apply_req(self, *args, **kwargs):
        return super(instrumented_dataframe_wrapper, self).process_apply_req(
            *args, **kwargs)

    @instrument("server.dataframe.apply_changes")
    def apply_changes(self, *args, **kwargs):
        return super(instrumented_dataframe_wrapper, self).apply_changes(
            *args, **kwargs)
