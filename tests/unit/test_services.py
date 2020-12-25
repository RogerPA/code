import pytest
from allocation.adapters import repository
from allocation.service_layer import services, unit_of_work


class FakeRepository(repository.AbstractRepository):

    def __init__(self, batches):
        self._batches = set(batches)

    def add(self, batch):
        self._batches.add(batch)

    def get(self, reference):
        return next(b for b in self._batches if b.reference == reference)

    def list(self):
        return list(self._batches)


class FakeUnitOfWork(unit_of_work.AbstractUnitOfWork):

    def __init__(self):
        self.batches = FakeRepository([])
        self.commited = False

    def commit(self):
        self.committed = True

    def rollback(self):
        pass


class FakeUnitOfWorkStater(unit_of_work.AbstractUnitOfWorkStarter):

    def __init__(self):
        self.unit_of_work = FakeUnitOfWork()

    def __enter__(self):
        return self.unit_of_work

    def __exit__(self, type, value, traceback):
        pass


def test_add_batch():
    # uow = FakeUnitOfWork()
    # fake_uow_starter = FakeUoWContextManager(uow) ?
    # fake_uow_starter = contextlib.nullcontext(uow) ?
    # services.add_batch("b1", "CRUNCHY-ARMCHAIR", 100, None, fake_uow_starter)
    fake_uow_starter = FakeUnitOfWorkStater()
    services.add_batch("b1", "CRUNCHY-ARMCHAIR", 100, None, fake_uow_starter)
    assert fake_uow_starter.unit_of_work.batches.get("b1") is not None
    assert fake_uow_starter.unit_of_work.committed


def test_allocate_returns_allocation():
    fake_uow_starter = FakeUnitOfWorkStater()
    services.add_batch("batch1", "COMPLICATED-LAMP", 100, None, fake_uow_starter)
    result = services.allocate("o1", "COMPLICATED-LAMP", 10, fake_uow_starter)
    assert result == "batch1"


def test_allocate_errors_for_invalid_sku():
    fake_uow_starter = FakeUnitOfWorkStater()
    services.add_batch("b1", "AREALSKU", 100, None, fake_uow_starter)

    with pytest.raises(services.InvalidSku, match="Invalid sku NONEXISTENTSKU"):
        services.allocate("o1", "NONEXISTENTSKU", 10, fake_uow_starter)


def test_allocate_commits():
    fake_uow_starter = FakeUnitOfWorkStater()
    services.add_batch("b1", "OMINOUS-MIRROR", 100, None, fake_uow_starter)
    services.allocate("o1", "OMINOUS-MIRROR", 10, fake_uow_starter)
    assert fake_uow_starter.unit_of_work.committed
