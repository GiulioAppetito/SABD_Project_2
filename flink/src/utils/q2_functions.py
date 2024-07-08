from pyflink.datastream.functions import AggregateFunction, ProcessAllWindowFunction
from pyflink.common import Row
from pyflink.datastream.functions import AggregateFunction, ProcessAllWindowFunction
from typing import List, Tuple, Dict, Iterable
import logging
from pyflink.datastream.functions import AggregateFunction, ProcessAllWindowFunction, ReduceFunction


class VaultFailuresPerDayReduceFunction(ReduceFunction):
    def reduce(self, value1: Tuple[int, int, List[str]], value2: Tuple[int, int, List[str]]) -> Tuple[
        int, int, List[str]]:
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

        # Check if the vault_id already exists in the ranking
        for i, (v_id, f_count, f_disks) in enumerate(ranking):
            if v_id == vault_id:
                # Update the entry if the current failures_count is higher
                if failures_count > f_count:
                    ranking[i] = (vault_id, failures_count, failed_disks)
                break
        else:
            # Add the new entry if vault_id is not found
            ranking.append((vault_id, failures_count, failed_disks))

        # Sort and keep top 10
        ranking.sort(key=lambda x: x[1], reverse=True)
        ranking = ranking[:10]

        return ranking

    def merge(self, ranking_a: List[Tuple[int, int, List[str]]], ranking_b: List[Tuple[int, int, List[str]]]) -> List[Tuple[int, int, List[str]]]:
        combined_ranking = {vault_id: (failures_count, failed_disks) for vault_id, failures_count, failed_disks in ranking_a}

        for vault_id, failures_count, failed_disks in ranking_b:
            if vault_id in combined_ranking:
                # Keep the entry with the maximum failures_count
                if failures_count > combined_ranking[vault_id][0]:
                    combined_ranking[vault_id] = (failures_count, failed_disks)
            else:
                combined_ranking[vault_id] = (failures_count, failed_disks)

        # Convert back to list and sort
        ranking = [(vault_id, data[0], data[1]) for vault_id, data in combined_ranking.items()]
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


def convert_to_row(x):
    ts, ranking = x
    row_data = [ts]
    for i in range(10):
        if i < len(ranking):
            vault_id, failures_count, failed_disks = ranking[i]
            failed_disks_str = ', '.join(failed_disks)  # Convert list to comma-separated string without brackets
            row_data.extend([vault_id, failures_count, failed_disks_str])
        else:
            row_data.extend([None, None, None])
    return Row(*row_data)

