from pyflink.datastream.functions import AggregateFunction, ProcessAllWindowFunction
from pyflink.common import Row
from pyflink.datastream.functions import AggregateFunction, ProcessAllWindowFunction
from typing import List, Tuple, Dict, Iterable
import logging
from pyflink.datastream.functions import AggregateFunction, ProcessAllWindowFunction, ReduceFunction


class VaultFailuresPerDayReduceFunction(ReduceFunction):
    def reduce(self, value1: Tuple[int, int, List[str]], value2: Tuple[int, int, List[str]]) -> Tuple[int, int, List[str]]:
        logging.info(f"Reducing: {value1} and {value2}")
        vault_id = value1[0]
        failures_count = value1[1] + value2[1]
        failed_disks = value1[2] + value2[2]
        result = (vault_id, failures_count, failed_disks)
        logging.info(f"Reduced Result: {result}")
        return result


class FailuresAggregateFunction(AggregateFunction):

    def create_accumulator(self) -> List[Tuple[int, int, List[str]]]:
        return []

    def add(self, value: Tuple[int, int, List[str]], ranking: List[Tuple[int, int, List[str]]]) -> List[Tuple[int, int, List[str]]]:
        vault_id = value[0]
        failures_count = value[1]
        failed_disks = value[2]

        ranking.append((vault_id, failures_count, failed_disks))
        ranking.sort(key=lambda x: x[1], reverse=True)
        ranking = ranking[:10]

        return ranking

    def merge(self, ranking_a: List[Tuple[int, int, List[str]]], ranking_b: List[Tuple[int, int, List[str]]]) -> List[Tuple[int, int, List[str]]]:
        ranking = ranking_a + ranking_b
        ranking.sort(key=lambda x: x[1], reverse=True)
        ranking = ranking[:10]

        return ranking

    def get_result(self, accumulator: List[Tuple[int, int, List[str]]]) -> List[Tuple[int, int, List[str]]]:
        return accumulator


class TimestampForVaultsRanking(ProcessAllWindowFunction):
    def process(
            self,
            context: ProcessAllWindowFunction.Context,
            elements: Iterable[List[Tuple[int, int, List[str]]]],
    ) -> Iterable[Tuple[int, List[Tuple[int, int, List[str]]]]]:
        ranking = next(iter(elements))
        window = context.window()

        result_row = (window.start, ranking)
        yield result_row
