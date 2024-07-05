from pyflink.datastream.functions import AggregateFunction, ReduceFunction, ProcessAllWindowFunction
from pyflink.datastream.window import TimeWindow
from typing import Tuple, List, Iterable
from pyflink.common import Row


class VaultFailuresPerDayReduceFunction(ReduceFunction):
    def reduce(self, value1: Row, value2: Row) -> Row:
        vault_id = value1['vault_id']
        failures_count = value1['failures_count'] + value2['failures_count']
        failed_disks = value1['failed_disks'] + value2['failed_disks']
        return Row(vault_id=vault_id, failures_count=failures_count, failed_disks=failed_disks)


class VaultFailuresAggregateFunction(AggregateFunction):
    def create_accumulator(self):
        return []

    def add(self, value: Row, accumulator) -> List[Tuple[int, int, List[str]]]:
        vault_id, failures_count, failed_disks = value
        accumulator.append((vault_id, failures_count, failed_disks))
        accumulator.sort(key=lambda x: x[1], reverse=True)
        return accumulator[:10]

    def get_result(self, accumulator):
        return accumulator

    def merge(self, ranking_a, ranking_b):
        ranking = ranking_a + ranking_b
        ranking.sort(key=lambda x: x[1], reverse=True)
        return ranking[:10]

    def get_result(self, accumulator):
        return accumulator


class VaultFailuresProcessFunction(ProcessAllWindowFunction):
    def process(
            self,
            context: ProcessAllWindowFunction.Context,
            elements: Iterable[List[Tuple[int, int, List[str]]]],
    ) -> Iterable[Row]:
        ranking = next(iter(elements))
        window: TimeWindow = context.window()

        row_data = [window.start]
        for i in range(10):
            if i < len(ranking):
                vault_id, failures_count, failed_disks = ranking[i]
                row_data.extend([vault_id, failures_count, failed_disks])
            else:
                row_data.extend([None, None, None])

        yield Row(*row_data)
