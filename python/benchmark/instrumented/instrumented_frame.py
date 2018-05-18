from spacetime.client.frame import ClientFrame
from benchmark.instrumented.instrument import instrument

class InstrumentedFrame(ClientFrame):
    def __init__(self, *args, **kwargs):
        self.instrumentation = dict()
        super(InstrumentedFrame, self).__init__(*args, **kwargs)
        self.is_instrumented = True

    @instrument("client.register")
    def _register_app(self):
        return super(InstrumentedFrame, self)._register_app()

    @instrument("client.one_step", queue=True)
    def _one_step(self):
        return super(InstrumentedFrame, self)._one_step()

    @instrument("client.one_step.pull")
    def _pull(self):
        return super(InstrumentedFrame, self)._pull()

    @instrument("client.one_step.pull.process_pull")
    def _process_pull_resp(self, only_diff, resp):
        return super(
            InstrumentedFrame, self)._process_pull_resp(only_diff, resp)

    @instrument("client.one_step.update")
    def _update(self):
        return super(InstrumentedFrame, self)._update()

    @instrument("client.one_step.push")
    def _push(self):
        return super(InstrumentedFrame, self)._push()

    @instrument("client.shutdown")
    def _shutdown(self):
        return super(InstrumentedFrame, self)._shutdown()

    @instrument("client.unregister")
    def _unregister_app(self):
        return super(InstrumentedFrame, self)._unregister_app()
