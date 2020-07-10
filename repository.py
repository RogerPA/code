import abc
import model


class AbstractRepository(abc.ABC):

    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError


class SqlRepository(AbstractRepository):

    def __init__(self, session):
        self.session = session

    def add(self, batch):
        batch_id = self.session.execute(
            f"SELECT id "
            f"FROM batches "
            f'WHERE reference="{batch.reference}" AND sku="{batch.sku}"',
        )
        result = batch_id.first()
        if result:
            batch_id = result.values()[0]
            self.session.execute(
                f"UPDATE batches "
                f'SET reference="{batch.reference}",'
                f'sku="{batch.sku}",'
                f'_purchased_quantity="{batch._purchased_quantity}",'
                f'eta=null WHERE id="{batch_id}"',
            )
        else:
            self.session.execute(
                f"INSERT INTO batches"
                f"(reference, sku, _purchased_quantity, eta)"
                f'VALUES ("{batch.reference}", "{batch.sku}",'
                f"{batch._purchased_quantity}, null)",
            )
            [[batch_id]] = self.session.execute(
                f"SELECT id "
                f"FROM batches "
                f'WHERE reference="{batch.reference}" '
                f'AND sku="{batch.sku}"',
            )

        for order_line in batch._allocations:
            orderline_id = self.session.execute(
                f"SELECT id "
                f"FROM order_lines "
                f'WHERE orderid="{order_line.orderid}" '
                f'AND sku="{order_line.sku}" '
                f'AND qty="{order_line.qty}"',
            )
            result = orderline_id.first()
            if result:
                orderline_id = result.values()[0]
                self.session.execute(
                    f"UPDATE order_lines "
                    f'SET orderid="{order_line.orderid}",'
                    f'sku="{order_line.sku}",'
                    f'qty="{order_line.qty}" '
                    f'WHERE id="{orderline_id}"',
                )
            else:
                self.session.execute(
                    f"INSERT INTO order_lines"
                    f"(orderid, sku, qty)"
                    f'VALUES ("{order_line.orderid}",'
                    f'"{order_line.sku}", "{order_line.qty}")',
                )
                [[orderline_id]] = self.session.execute(
                    f"SELECT id "
                    f"FROM order_lines "
                    f'WHERE orderid="{order_line.orderid}" '
                    f'AND sku="{order_line.sku}"',
                )
            self.session.execute(
                f"INSERT INTO allocations"
                f"(orderline_id, batch_id)"
                f'VALUES ("{orderline_id}",'
                f'"{batch_id}")',
            )

    def get(self, reference) -> model.Batch:
        [batch] = self.session.execute(
            f'SELECT * FROM batches WHERE reference="{reference}"',
        )
        batch_obj = model.Batch(
            batch[1],
            batch[2],
            batch[3],
            batch[4],
        )
        [order_line_ids] = self.session.execute(
            f'SELECT orderline_id FROM allocations where batch_id="{batch[0]}"',
        )
        for order_line_id in order_line_ids:
            if order_line_id:
                [order_line] = self.session.execute(
                    f'SELECT * FROM order_lines WHERE id="{order_line_id}"',
                )
                order_line_obj = model.OrderLine(
                    order_line[3],
                    order_line[1],
                    order_line[2],
                )
                batch_obj._allocations.add(order_line_obj)
        return batch_obj
