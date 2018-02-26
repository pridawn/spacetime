from benchmark.instrumented.instrument import instrument
from spacetime.connectors.mysql import MySqlConnection
from spacetime.connectors.spacetime import SpacetimeConnection, ObjectlessSpacetimeConnection
# pylint: disable=W0221

class InstrumentedMySqlConnection(MySqlConnection):
    @instrument("client.register.connection")
    def register(self, *args, **kwargs):
        return super(
            InstrumentedMySqlConnection, self).register(*args, **kwargs)

    @instrument("client.one_step.push.post_update")
    def update(self, *args, **kwargs):
        return super(
            InstrumentedMySqlConnection, self).update(*args, **kwargs)

    @instrument("client.one_step.pull.get_update")
    def get_updates(self, *args, **kwargs):
        return super(
            InstrumentedMySqlConnection, self).get_updates(*args, **kwargs)

    @instrument("client.unregister.disconnect")
    def disconnect(self, *args, **kwargs):
        return super(
            InstrumentedMySqlConnection, self).disconnect(*args, **kwargs)


class InstrumentedSpacetimeConnection(SpacetimeConnection):
    @instrument("client.register.connection")
    def register(self, *args, **kwargs):
        return super(
            InstrumentedSpacetimeConnection, self).register(*args, **kwargs)

    @instrument("client.one_step.push.post_update")
    def update(self, *args, **kwargs):
        return super(
            InstrumentedSpacetimeConnection, self).update(*args, **kwargs)

    @instrument("client.one_step.pull.get_update")
    def get_updates(self, *args, **kwargs):
        return super(
            InstrumentedSpacetimeConnection, self).get_updates(*args, **kwargs)

    @instrument("client.unregister.disconnect")
    def disconnect(self, *args, **kwargs):
        return super(
            InstrumentedSpacetimeConnection, self).disconnect(*args, **kwargs)


class InstrumentedObjectlessSpacetimeConnection(ObjectlessSpacetimeConnection):
    @instrument("client.register.connection")
    def register(self, *args, **kwargs):
        return super(
            InstrumentedObjectlessSpacetimeConnection, self).register(
                *args, **kwargs)

    @instrument("client.one_step.push.post_update")
    def update(self, *args, **kwargs):
        return super(
            InstrumentedObjectlessSpacetimeConnection, self).update(
                *args, **kwargs)

    @instrument("client.one_step.pull.get_update")
    def get_updates(self, *args, **kwargs):
        return super(
            InstrumentedObjectlessSpacetimeConnection, self).get_updates(
                *args, **kwargs)

    @instrument("client.unregister.disconnect")
    def disconnect(self, *args, **kwargs):
        return super(
            InstrumentedObjectlessSpacetimeConnection, self).disconnect(
                *args, **kwargs)
